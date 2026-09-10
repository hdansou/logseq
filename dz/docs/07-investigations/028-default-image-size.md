# Default Image Size Investigation

> **Last revised:** 2026-09-10
> **Verified against:** `master` @ `be800f1` (2026-09-08). Source read only; the CSS workaround was not tested in the app. The resize values quoted below were observed in a real graph and supplied on 2026-09-10.
> **Context:** a user wants images to appear at a chosen size by default instead of resizing each one, ideally set in `config.edn`.

## Question

Can I set a default display size for images? Resizing an image sets asset properties. I want a default that applies without resizing, for example `:logseq.property.asset/resize-metadata {:width 666}` in `config.edn`.

## Answer

No setting exists. An image that has never been resized renders **250px wide**, and that number is hard-coded (`src/main/frontend/components/block.cljs:1161`). `config.edn` has no image-size key and the renderer never reads config for this, so adding `:logseq.property.asset/resize-metadata {:width 666}` to `config.edn` does nothing.

Two ways forward:

- **Today:** a `custom.css` rule that restyles only un-resized images ([below](#workaround-customcss)).
- **Properly:** a code change that reads a new `config.edn` key in place of the 250 ([below](#other-routes)).

## Findings

### Only one of the asset properties changes when you resize

| Property | Holds | Set by |
|---|---|---|
| `:logseq.property.asset/width`, `:logseq.property.asset/height` | The image file's original pixel size | Measured once on first render, only if missing (`block.cljs:231-242`). Hidden, `:raw-number` (`deps/db/src/logseq/db/frontend/property.cljs:575-584`) |
| `:logseq.property.asset/resize-metadata` | `{:width N}`, the display width | Saved when a resize drag ends (`src/main/frontend/handler/editor.cljs:2219-2224`). Hidden, not public, `:map` (`property.cljs:601-604`) |

The resize is saved on the asset entity whenever there is one (`editor.cljs:2221-2222`), not on the block that shows it. Resizing an image in one place changes it everywhere it appears.

### How the display width is chosen

In `asset-cp` (`block.cljs:1131`):

1. `width` is read from `resize-metadata` (`block.cljs:1157`).
2. The display width is `(or width 250 asset-width)` (`block.cljs:1161`). Because 250 is always truthy, `asset/width` is never used as a width. It only supplies the aspect ratio, and height is width divided by that ratio (`block.cljs:1162-1168`).
3. The loading placeholder gets the same size as an inline style (`block.cljs:1169-1171`).

`resizable-image` then merges `resize-metadata` into that metadata (`block.cljs:404`; `components/block/image.cljs:4-11`). `asset-container` merges the metadata into the `<img>` attributes (`block.cljs:261-268`), so the width is an HTML `width` attribute, not an inline style.

### What a resize writes

The handler in `resizable-image` (`block.cljs:443-458`):

- **Drag start:** records the `.ls-resize-image` frame's `offsetWidth`, a whole number of pixels (`:443-446`).
- **Drag move:** adds the pointer's `dx`, which is often fractional on HiDPI screens and trackpads. It won't shrink below 60px (`:447-451`).
- **Drag end:** saves the unrounded total through `resize-image!` (`:452-458`).

Observed in a real graph: `{:width 675.1597900390625}`, then `{:width 540.3753356933594}` after resizing again.

Resizing is unavailable on native mobile, in breadcrumbs, in positioned properties, in table view, and wherever `:disable-resize?` is set (`block.cljs:410-421`).

### No config.edn option

- The `config.edn` template's only asset-related entries are `:custom-css-url` (`deps/common/resources/templates/config.edn:50-53`) and a note about media URLs (`:219`).
- `asset-cp` reads no config when sizing images.

## Workaround: custom.css

```css
/* Default display width for images that haven't been resized */
.asset-container img[width="250"] {
  width: 666px;   /* preferred default */
  max-width: 100%;
  height: auto;
}
```

**Why it works.** An HTML `width` attribute has lower priority than any CSS rule, so the rule overrides it. `[width="250"]` matches only un-resized images, because dragged widths are fractional and almost never exactly 250. Once an image is resized, the rule stops matching and the saved size wins. Drag-resizing starts from the frame's current width, so it begins from the size you see.

**Where to put it.** Settings → General → Edit custom.css (`components/settings.cljs:217-221`, `:885`). A DB graph stores `logseq/custom.css` inside the graph, and `:custom-css-url` overrides it (`handler/ui.cljs:114-116`).

**Limits:**

- Nothing is saved. Un-resized images still have no `resize-metadata`.
- The loading placeholder is still 250px (inline style), so the layout can jump when the image appears.
- Images inside property values are capped at 35% width (`block.css:1999-2001`).
- Breadcrumb images are forced to 18px tall with `width: unset !important` (`block.css:98-104`).
- Table view caps images at 80px wide and 200px tall (`table.css:6-8`).

## Other routes

- **Plugin (unverified).** The plugin API passes fully qualified property names through unchanged (`src/main/logseq/api/block.cljs:44-57`), so a plugin could try writing `:logseq.property.asset/resize-metadata` on new assets. Whether a write to that hidden built-in property is accepted, and whether a JS object arrives in the right map shape, was not checked.
- **Code change.** Replace the hard-coded 250 at `block.cljs:1161` with a new `config.edn` key. Scoped in [Default Image Width in config.edn](../10-agent-guide/proposed/2026-09-10-config-default-image-width.md) as `:ui/default-image-width`. This belongs on a branch off master.

## Not verified

- The CSS rule was not tried in the running app.
- The plugin write path above.
- Markdown image-link metadata (`![](../assets/x.png){:width 600}`), handled by `image-link` (`block.cljs:641-668`), was not examined for DB graphs.

## Related

- [Asset management how-to](../ddocs/how-to/multimedia/asset-management.md)
- [Custom CSS customization guide](custom-css-customization-guide.md)
- [026 — Default Paths by Platform](026-default-paths-by-platform.md) (where `custom.css` lives)
