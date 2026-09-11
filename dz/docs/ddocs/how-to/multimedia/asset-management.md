---
title: 'Asset Management in Logseq'
status: '🟢 Stable'
version: 'DB graph schema 65.33'
quadrant: 'How-To Guide'
platform: 'Desktop + Web + Mobile'
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
related-docs:
  - '../../reference/features/asset-sync.md'
  - '../multimedia/video-embed.md'
  - '../../reference/database/schema-overview.md'
  - '../../tutorials/getting-started/03-basic-editing.md'
  - '../organization/backup-strategies.md'
---

# Asset Management in Logseq

This guide shows you how to add, view, resize, find, delete and back up assets in a Logseq DB graph. Assets are files you attach to a graph: images, PDFs, audio, video and any other file.

In a DB graph every asset is a **block** tagged `Asset`, and its file is stored as `assets/<block uuid>.<extension>` in the graph folder. The block holds the title and metadata. The file holds the bytes.

For link syntax, the full format lists and what sync replicates, see [Assets: Link Syntax, File Types, and Sync](../../reference/features/asset-sync.md).

---

## Quick Start

### Add a file

| Method | How | Source |
| ------ | --- | ------ |
| **Slash command** | Type `/Upload an asset` ("Upload file types like image, PDF, DOCX, etc.") and pick a file | [`en.edn:568-569`](../../../../../src/resources/dicts/en.edn); [`commands.cljs:335-338`](../../../../../src/main/frontend/commands.cljs); [`components/editor.cljs:495-504`](../../../../../src/main/frontend/components/editor.cljs) |
| **Paste** | Paste a file or screenshot while editing a block | [`handler/paste.cljs:272-281`, `:298-300`](../../../../../src/main/frontend/handler/paste.cljs) |
| **Drag and drop** | Drop files onto a block | [`components/block/drop.cljs:12-29`](../../../../../src/main/frontend/components/block/drop.cljs) |
| **Asset tag page** | Open the **Asset** tag page, click its upload button, and pick files in the **Add assets** dialog | [`components/objects.cljs:79-92`](../../../../../src/main/frontend/components/objects.cljs); [`components/views.cljs:1964-1976`](../../../../../src/main/frontend/components/views.cljs); [`en.edn:69`](../../../../../src/resources/dicts/en.edn) |
| **Mobile camera** | Tap **Photo** in the editor toolbar to take or choose a photo | [`src/main/mobile/components/editor_toolbar.cljs:141-149`](../../../../../src/main/mobile/components/editor_toolbar.cljs); [`frontend/mobile/camera.cljs:32-41`, `:61-72`](../../../../../src/main/frontend/mobile/camera.cljs); [`en.edn:1059`](../../../../../src/resources/dicts/en.edn) |

The file picker has no type filter, so any file with an extension is accepted ([`components/editor.cljs:498-504`](../../../../../src/main/frontend/components/editor.cljs)).

### What happens when you add a file

Every method ends in `db-based-save-assets!` → `new-asset-block` ([`src/main/frontend/handler/editor.cljs:1760-1869`](../../../../../src/main/frontend/handler/editor.cljs)):

1. **Duplicate check.** Logseq computes the file's SHA-256 checksum ([`handler/assets.cljs:186-189`](../../../../../src/main/frontend/handler/assets.cljs); [`deps/db/src/logseq/db/frontend/asset.cljs:12-19`](../../../../../deps/db/src/logseq/db/frontend/asset.cljs)). If an asset with the same checksum exists, nothing is added and you see "Asset exists already, title: {1}, node reference: [[{2}]]" ([`editor.cljs:1770-1778`](../../../../../src/main/frontend/handler/editor.cljs); [`en.edn:81`](../../../../../src/resources/dicts/en.edn)). The file name does not matter.
2. **Checks.** A file with no extension is rejected ("File doesn't have a valid ext."). A file over 100 MiB shows "Asset size shouldn't be larger than 100M" ([`editor.cljs:1781-1789`](../../../../../src/main/frontend/handler/editor.cljs); [`handler/assets.cljs:14-17`](../../../../../src/main/frontend/handler/assets.cljs); [`en.edn:109`](../../../../../src/resources/dicts/en.edn)).
3. **File.** The bytes are written to `assets/<uuid>.<ext>` ([`editor.cljs:1790-1793`](../../../../../src/main/frontend/handler/editor.cljs)).
4. **Block.** A block is created with the tag `Asset`, a title and the properties below ([`editor.cljs:1794-1802`](../../../../../src/main/frontend/handler/editor.cljs)). The title is the file name without its extension. A pasted file named `image` (a typical screenshot) gets the current date and time as its title instead ([`editor.cljs:1765-1769`](../../../../../src/main/frontend/handler/editor.cljs)).
5. **Placement.** The new block goes next to the block you are editing (replacing it if it is empty), otherwise at the end of today's journal ([`editor.cljs:1819-1864`](../../../../../src/main/frontend/handler/editor.cljs)).

> **Note:** The extension check throws inside the upload promise and is only logged to the console ([`editor.cljs:1883-1885`](../../../../../src/main/frontend/handler/editor.cljs)). If an extensionless file seems to do nothing, that is why. Rename it with an extension.

> Removed claim: "Markdown link inserted: `![](../assets/image.png)`". Adding a file creates an asset block, not a Markdown link. Also removed: the `/Upload an image` command (the command is **Upload an asset**) and "Spaces replaced with hyphens / `my-image_<timestamp>.png`" naming, which was file-graph behaviour.

---

## Asset Storage

### Where files live

| Platform | Location | Source |
| -------- | -------- | ------ |
| Desktop | `~/logseq/graphs/<graph folder>/assets/<uuid>.<ext>` | [`src/main/frontend/fs.cljs:118-126`](../../../../../src/main/frontend/fs.cljs); [`deps/common/src/logseq/common/config.cljs:55-56`](../../../../../deps/common/src/logseq/common/config.cljs) |
| Browser | In-browser file storage under `memory:///<graph>/assets/` | [`src/main/frontend/worker/platform/browser.cljs:106-111`](../../../../../src/main/frontend/worker/platform/browser.cljs) |

The file name comes from the block, so renaming the asset's title does not rename the file ([`src/main/frontend/components/block/asset.cljs:37-43`](../../../../../src/main/frontend/components/block/asset.cljs)).

### The asset block

The `Asset` tag is defined in [`deps/db/src/logseq/db/frontend/class.cljs:68-74`](../../../../../deps/db/src/logseq/db/frontend/class.cljs):

```clojure
:logseq.class/Asset
{:title "Asset"
 :properties {:logseq.property.class/hide-from-node true
              :logseq.property.view/type :logseq.property.view/type.gallery}
 :schema {:properties [:logseq.property.asset/type :logseq.property.asset/size :logseq.property.asset/checksum]
          :required-properties [:logseq.property.asset/type :logseq.property.asset/size :logseq.property.asset/checksum]}}
```

`hide-from-node` hides the `Asset` tag chip on asset blocks ([`block.cljs:2883-2889`](../../../../../src/main/frontend/components/block.cljs)). The tag page shows its assets as a gallery by default.

**Properties** ([`deps/db/src/logseq/db/frontend/property.cljs:554-609`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)):

| Property | Holds | Set by |
| -------- | ----- | ------ |
| `:logseq.property.asset/type` | Lower-cased extension, e.g. `"png"` | Upload ([`asset.cljs:21-24`](../../../../../deps/db/src/logseq/db/frontend/asset.cljs)) |
| `:logseq.property.asset/size` | Size in bytes | Upload |
| `:logseq.property.asset/checksum` | SHA-256 hex digest | Upload |
| `:logseq.property.asset/external-url` | URL or path for an asset whose file is not in `assets/` | The asset form ([`components/assets.cljs:225-278`](../../../../../src/main/frontend/components/assets.cljs)) |
| `:logseq.property.asset/external-file-name` | File name, used for Zotero PDFs | [`block.cljs:513-519`](../../../../../src/main/frontend/components/block.cljs) |
| `:logseq.property.asset/width`, `…/height` | Natural image size in pixels, recorded the first time the image renders | [`block.cljs:231-243`](../../../../../src/main/frontend/components/block.cljs) |
| `:logseq.property.asset/resize-metadata` | The width you dragged the image to | [`editor.cljs:2219-2224`](../../../../../src/main/frontend/handler/editor.cljs) |
| `:logseq.property.asset/align` | `:left`, `:center` or `:right` | [`block.cljs:327-332`](../../../../../src/main/frontend/components/block.cljs) |
| `:logseq.property.asset/last-visit-page` | Last page viewed in a PDF (not synced) | [`property.cljs:589-593`](../../../../../deps/db/src/logseq/db/frontend/property.cljs) |
| `:logseq.property.asset/remote-metadata` | `{:checksum … :type …}` after a successful sync upload | [`src/main/frontend/worker/sync/assets.cljs:257-266`](../../../../../src/main/frontend/worker/sync/assets.cljs) |

All of these are hidden properties except `external-url` ([`property.cljs:554-609`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)).

> Removed section: "File-Based Graphs" storage layout (`pages/`, `logseq/`, timestamped asset names). File graphs can no longer be opened in the app ([`src/main/frontend/components/repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)). Also removed: the claim that `checksum` is stored as `"sha256:abcd..."`. It is the plain hex digest.

---

## Viewing Assets

### How each type renders

The asset's extension picks the renderer ([`block.cljs:581-637`](../../../../../src/main/frontend/components/block.cljs)):

| Extension | Renders as |
| --------- | ---------- |
| Audio: `mp3 ogg mpeg wav m4a flac wma aac` | Audio player |
| Video: `mp4 webm mov flv avi mkv` | Video player |
| Image: `gif svg jpeg ico png jpg bmp webp avif cr2` | Inline image |
| `pdf` | A link that opens the built-in PDF viewer |
| Anything else | Desktop: a link that opens the file in its default app. Browser: a download link. Native mobile: opens or shares the file |

(Format sets: [`src/main/frontend/config.cljs:174-186`](../../../../../src/main/frontend/config.cljs); image set: [`deps/common/src/logseq/common/config.cljs:96-98`](../../../../../deps/common/src/logseq/common/config.cljs).) A `.webm` whose title starts with `Audio-` plays as audio ([`block.cljs:582-584`](../../../../../src/main/frontend/components/block.cljs)).

Asset blocks for images, audio and video show the media inside the block, together with the block's title ([`block.cljs:1213-1218`, `:3643-3650`](../../../../../src/main/frontend/components/block.cljs)). A PDF asset block shows its title as the viewer link ([`block.cljs:2574-2576`](../../../../../src/main/frontend/components/block.cljs)). `.heic` is not in the image set, so it shows as a file link.

### Image actions

- **Click an image** to open it in a lightbox with the other images on screen ([`block.cljs:150-168`, `:256-259`](../../../../../src/main/frontend/components/block.cljs)).
- **The ⋮ button** on an image asset offers ([`block.cljs:333-392`](../../../../../src/main/frontend/components/block.cljs); strings in [`en.edn:77-80`, `:84`, `:88`, `:96`, `:108`](../../../../../src/resources/dicts/en.edn)):
  - **Align** → Align left / Align center / Align right
  - **Copy image**. In the desktop app only `png jpg jpeg gif webp bmp svg ico` can be copied. Other types show "Copy image is not supported for {1} files" ([`block.cljs:276-289`](../../../../../src/main/frontend/components/block.cljs); [`en.edn:85`](../../../../../src/resources/dicts/en.edn)).
  - **Show file in folder** (a local asset) or **Open image in browser** (a remote one), desktop app only.
  - **Delete image** (see [Deleting Assets](#deleting-assets)). Not shown in published pages.

### Resizing images

- Drag the image's edge to resize it. The width is saved to the asset's `resize-metadata` ([`editor.cljs:2219-2224`](../../../../../src/main/frontend/handler/editor.cljs)), so every reference to that asset uses it.
- An image you have not resized displays **250 px** wide. The value is hard-coded, with no setting ([`block.cljs:1157-1168`](../../../../../src/main/frontend/components/block.cljs)).
- Resizing is off in the native mobile app, in breadcrumbs, for images shown in a property position and in table views ([`block.cljs:410-422`](../../../../../src/main/frontend/components/block.cljs)).

> Removed claim: `![Diagram](../assets/diagram.png){:width 600 :height 400}`. Not verified for DB graphs. Drag to resize instead.

### Missing files and sync progress

- If an asset's file is not in `assets/` and the asset has no external URL and no sync record, the block shows "Can't find file {1}." with a **Reload file** button ([`block.cljs:1118-1129`, `:1199-1203`](../../../../../src/main/frontend/components/block.cljs); [`components/block/asset.cljs:45-53`](../../../../../src/main/frontend/components/block/asset.cljs); [`en.edn:95`, `:104`](../../../../../src/resources/dicts/en.edn)).
- In a synced graph, an asset being transferred shows "Uploading" or "Downloading" with a percentage ([`block.cljs:1141-1154`](../../../../../src/main/frontend/components/block.cljs); [`en.edn:89`, `:112-113`](../../../../../src/resources/dicts/en.edn)).

### Video embeds

`{{video URL}}`, `{{youtube …}}`, `{{vimeo …}}` and `{{bilibili …}}` embed remote video ([`src/main/frontend/components/block/video.cljs:6`](../../../../../src/main/frontend/components/block/video.cljs); [`block.cljs:1879-1904`](../../../../../src/main/frontend/components/block.cljs)). They do not create assets. See [Video Embedding](../multimedia/video-embed.md).

### PDFs

The PDF link opens the built-in viewer ([`block.cljs:513-527`, `:592-601`](../../../../../src/main/frontend/components/block.cljs)). When you create an asset from a PDF that was opened by URL, the asset form warns: "Creating a local asset from an external one. PDF annotations require a local asset to work properly." ([`components/assets.cljs:280-299`](../../../../../src/main/frontend/components/assets.cljs); [`en.edn:86`](../../../../../src/resources/dicts/en.edn)). See [PDF Annotations](../../reference/features/pdf-annotations.md).

---

## Finding Assets

**All assets:** open the **Asset** tag page. It lists every asset as a gallery ([`class.cljs:68-72`](../../../../../deps/db/src/logseq/db/frontend/class.cljs)).

**With a simple query:** `{{query …}}` does not run in a DB graph. It renders a deprecation warning ([`block.cljs:1870-1871`](../../../../../src/main/frontend/components/block.cljs)). Type `/Query` and enter the query bare:

```clojure
(and (tags Asset) (private-property :logseq.property.asset/type "png"))
```

```clojure
(and (tags Asset) (tags ui))
```

The first lists PNG assets. The second lists assets you have tagged `#ui`. `asset/type` is a non-public property ([`property.cljs:554-558`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)), and `(property …)` only matches public properties, so use `private-property` ([`src/main/frontend/worker/query_dsl.cljs:554-558`](../../../../../src/main/frontend/worker/query_dsl.cljs); [`deps/db/src/logseq/db/frontend/rules.cljc:188-194`, `:221-225`](../../../../../deps/db/src/logseq/db/frontend/rules.cljc)). `(tags Asset)` also matches tags that extend `Asset` ([`query_dsl.cljs:431-450`](../../../../../src/main/frontend/worker/query_dsl.cljs)).

> **Unverified:** both queries are traced from the query-DSL source and were not run.

> Removed: `{{query (and [[Assets]] #ui)}}` and `{{query (and [[logseq.class/Asset]] (property :logseq.property.asset/type "png"))}}`. The macro is deprecated, and `property` would not match the non-public `asset/type` property.

---

## Deleting Assets

**⋮ → Delete image** asks "Are you sure you want to delete this image?" ([`block.cljs:300-326`](../../../../../src/main/frontend/components/block.cljs); [`en.edn:83`](../../../../../src/resources/dicts/en.edn)). The result depends on where the image is shown ([`editor.cljs:1741-1753`](../../../../../src/main/frontend/handler/editor.cljs)):

- **On the asset block itself:** the asset block is deleted.
- **As a reference inside another block:** the reference is removed from that block's text. The dialog also shows a checkbox, **Remove the file too (notice it can't be restored)**, ticked by default ([`en.edn:97`](../../../../../src/resources/dicts/en.edn)). When it is ticked, the asset block is deleted as well.

You can also select rows on the **Asset** tag page and delete them. That deletes the asset blocks ([`src/main/frontend/components/views.cljs:1208-1222`](../../../../../src/main/frontend/components/views.cljs)).

**What happens next:**

- **The block does not go to Recycle.** Deleted blocks are retracted outright ([`deps/outliner/src/logseq/outliner/core.cljs:440-442`](../../../../../deps/outliner/src/logseq/outliner/core.cljs)). Undo with `mod+z` straight away.
- **The file stays in `assets/`**, despite the checkbox label. Deletion only removes the block. The worker's platform `asset-delete!` function is never called ([`src/main/frontend/worker/platform/node.cljs:554-560`, `:730-731`](../../../../../src/main/frontend/worker/platform/node.cljs); [`browser.cljs:158`, `:219`](../../../../../src/main/frontend/worker/platform/browser.cljs)).
- **In a synced graph, the uploaded copy is not deleted from the server.** Only `:update-asset` sync operations are ever created ([`src/main/frontend/worker/sync/asset_db_listener.cljs:17-25`](../../../../../src/main/frontend/worker/sync/asset_db_listener.cljs); [`src/main/frontend/worker/sync/client_op.cljs:607-617`](../../../../../src/main/frontend/worker/sync/client_op.cljs)). A `:remove-asset` operation is handled, but nothing produces one ([`worker/sync/assets.cljs:284-295`](../../../../../src/main/frontend/worker/sync/assets.cljs)).

**To free disk space**, delete the file from `assets/` yourself after deleting its block. The file name is `<uuid>.<ext>`, where the uuid is the deleted block's.

> Removed claims: "File removed from `assets/` folder automatically" and the "🟡 Planned" unused-asset cleanup and batch operations (bulk move, format conversion, compression). No such code exists.

---

## Moving Assets Between Graphs

- **One file:** upload the original file again in the other graph (any method in [Quick Start](#quick-start)). Copying a file into another graph's `assets/` folder does not create an asset, because nothing scans that folder ([asset-sync: Copying a file into `assets/`](../../reference/features/asset-sync.md#sync-eligibility)).
- **A whole graph with its assets:** **Export both SQLite DB and assets**, then **Import → SQLite + assets (.zip)** as a new graph ([Backup and Recovery Strategies](../organization/backup-strategies.md#scenario-2-corrupted-or-broken-graph)).

> **Unverified:** that ⋮ → **Copy image** followed by a paste in another graph creates an asset there. Copy puts an image blob on the clipboard ([`block.cljs:272-299`](../../../../../src/main/frontend/components/block.cljs)), and paste uploads clipboard files ([`paste.cljs:272-281`](../../../../../src/main/frontend/handler/paste.cljs)). The round trip was not tested.

> Removed: "Export page with assets (Settings → Export)". No such export exists.

---

## Remote Graphs (Sync)

> Sync replicates **every** file type. There is no extension allowlist. What it will not do is pick up a file copied into `assets/` by hand: sync reads asset *entities*, never the folder. See [Assets: Link Syntax, File Types, and Sync](../../reference/features/asset-sync.md).

**Upload:** when an asset's checksum is written, the sync listener queues an `:update-asset` operation ([`asset_db_listener.cljs:17-25`](../../../../../src/main/frontend/worker/sync/asset_db_listener.cljs)). The worker reads `assets/<uuid>.<type>` and uploads it ([`worker/sync/assets.cljs:145-172`](../../../../../src/main/frontend/worker/sync/assets.cljs)):

1. **E2EE graphs only:** the bytes are encrypted with the graph's AES key using AES-GCM ([`worker/sync/assets.cljs:26-33`, `:164-168`](../../../../../src/main/frontend/worker/sync/assets.cljs); [`src/main/frontend/common/crypt.cljs:194-200`](../../../../../src/main/frontend/common/crypt.cljs)). Other graphs upload the bytes as they are.
2. After a successful upload, the asset gets `:logseq.property.asset/remote-metadata` ([`worker/sync/assets.cljs:257-266`](../../../../../src/main/frontend/worker/sync/assets.cljs)).

**Download:** other devices fetch assets whose file is missing locally ([`worker/sync/assets.cljs:449`](../../../../../src/main/frontend/worker/sync/assets.cljs)), and an asset block with a missing file requests its download when it renders ([`block.cljs:1098-1108`](../../../../../src/main/frontend/components/block.cljs); [`handler/assets.cljs:301-316`](../../../../../src/main/frontend/handler/assets.cljs)).

**Size limits:**

| Where | Limit | Result | Source |
| ----- | ----- | ------ | ------ |
| Adding a file | 100 MiB | Refused with a warning | [`handler/assets.cljs:14-17`](../../../../../src/main/frontend/handler/assets.cljs) |
| Sync client | 100 MiB | The upload is **silently skipped**, and the asset stays local | [`worker/sync/assets.cljs:19`, `:246-255`](../../../../../src/main/frontend/worker/sync/assets.cljs) |
| Sync server | 100 MiB, or 200 MiB for E2EE graphs | `413 asset too large` | [`deps/db-sync/src/logseq/db_sync/worker/handler/assets.cljs:9-10`, `:226-231`](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/assets.cljs) |

The client limit applies to E2EE graphs too, so the server's 200 MiB allowance is not reached from the app. Full table in [Sync Limits](../../reference/features/sync-limits.md).

> Removed: the code excerpt `new-task--rtc-upload-asset` cited at `handler/assets.cljs:301-334`, the "Broadcast asset metadata to collaborators" step, and "AES-256" for every upload. That function does not exist at this revision; upload is `upload-remote-asset!` in the sync worker, and encryption applies only to E2EE graphs.

---

## Other Ways to Show Files

**Remote images:** `![Logo](https://example.com/logo.png)` shows the image, and so does a bare image URL. No file is downloaded and no asset is created, so the image is not in exports or backups and breaks if the URL goes away ([asset-sync: Remote http(s) URLs](../../reference/features/asset-sync.md#link-syntax)).

**Files outside the graph (desktop):** `![t](assets:///absolute/path.png)` shows a local file that is not an asset. See [asset-sync: Link syntax](../../reference/features/asset-sync.md#link-syntax) for which path forms work.

**Data URLs:** `![](data:image/png;base64,…)` renders inline ([`block.cljs:660-661`](../../../../../src/main/frontend/components/block.cljs)). The data is stored in the block's text inside `db.sqlite`, so keep it small.

> Removed section: "Asset Aliases (Desktop Only) → Settings → Assets". The Assets settings panel still exists in source ([`src/main/frontend/components/settings.cljs:1535-1536`](../../../../../src/main/frontend/components/settings.cljs)), but it is not in the Settings tab list ([`settings.cljs:1476-1494`](../../../../../src/main/frontend/components/settings.cljs)), so it cannot be opened. Aliases resolve only if the option was already on in this app's local storage ([`src/main/frontend/state.cljs:249`](../../../../../src/main/frontend/state.cljs); [`handler/assets.cljs:19-22`](../../../../../src/main/frontend/handler/assets.cljs)).

---

## Platform Notes

**Desktop app:** every feature above. Only the desktop app offers **Show file in folder** / **Open image in browser**, opening non-media files in their default app, and `assets://` paths.

**Browser:** assets live in browser storage, not a folder you can open ([`browser.cljs:106-111`](../../../../../src/main/frontend/worker/platform/browser.cljs)). Non-media files are download links. Keep a zip export as your copy of the files.

**Native mobile app:**

- Image resizing is off ([`block.cljs:410-412`](../../../../../src/main/frontend/components/block.cljs)).
- The **Photo** button takes or chooses a photo and also saves it to the device gallery. The `config.edn` key `:mobile/photo` accepts `:allow-editing?` and `:quality` (default 80) ([`camera.cljs:32-41`](../../../../../src/main/frontend/mobile/camera.cljs)).
- Files shared into Logseq from another app become assets only for document and media types (`doc-formats` plus `media-formats`). Other types show an unsupported-type notice ([`src/main/frontend/mobile/intent.cljs:136-152`, `:190-202`](../../../../../src/main/frontend/mobile/intent.cljs)).

> Removed: "PDF viewing limited (no annotations)", "Video playback (codec-dependent)", "External windows for PDFs" and "CORS issues" as platform features. None were traced to source.

---

## Troubleshooting

**"Can't find file {1}."**
The asset block exists but `assets/<uuid>.<ext>` is missing, for example after copying only `db.sqlite` to another machine. Restore the file from a zip export or a copy of `assets/`, then click **Reload file**. In a synced graph, click **Reload file** once the file is back in place to retry the upload ([`block.cljs:1110-1116`](../../../../../src/main/frontend/components/block.cljs)).

**"Asset exists already, title: …, node reference: [[…]]"**
A byte-identical file is already in the graph. Reference that asset instead ([`editor.cljs:1772-1778`](../../../../../src/main/frontend/handler/editor.cljs)).

**"Asset size shouldn't be larger than 100M"**
The file is over 100 MiB. Shrink or split it, or link to it remotely.

**Nothing happens when adding a file**
The file probably has no extension (see the Note in [Quick Start](#quick-start)). Check the developer console: the desktop app's **View** menu uses Electron's standard `viewMenu` role ([`src/electron/electron/core.cljs:243`](../../../../../src/electron/electron/core.cljs)).

**An asset shows on one device but not another (sync)**
Check that the asset has `remote-metadata`. If the file is over 100 MiB it was never uploaded ([`worker/sync/assets.cljs:246-255`](../../../../../src/main/frontend/worker/sync/assets.cljs)). Files copied into `assets/` by hand never sync.

**`.heic` photo shows as a file link**
`heic` is not in the image set that renders inline ([`deps/common/src/logseq/common/config.cljs:96-98`](../../../../../deps/common/src/logseq/common/config.cljs)). Convert it to JPEG or PNG before adding it.

**`assets/` keeps growing after deleting assets**
Deleting an asset block leaves its file behind (see [Deleting Assets](#deleting-assets)).

> Removed: "Verify path `../assets/filename.ext` (relative to page)", "Ensure `assets/` folder synced via Git/Dropbox", "Duplicate assets with timestamps" and "Disk space full / Permission denied" advice. These described file graphs or were generic.

---

## Backups

- **Automatic desktop backups and Export SQLite DB do not include assets.** Only **Export both SQLite DB and assets** bundles them ([Backup and Recovery Strategies](../organization/backup-strategies.md#method-4-export-sqlite-db-or-sqlite--assets)).
- The zip takes every file in `assets/`, including files left behind by deleted assets ([`handler/assets.cljs:269-281`](../../../../../src/main/frontend/handler/assets.cljs); [`src/main/frontend/handler/export.cljs:88-100`](../../../../../src/main/frontend/handler/export.cljs)).
- When you copy a graph folder yourself, copy `assets/` with it.

> Removed: "Git: commit `assets/`", the `.gitignore` example, "Keep total assets < 5 GB", "Compress images (< 500 KB)", "Don't embed videos > 50 MB", the ImageOptim / TinyPNG / ffmpeg recipes and the "Asset Inventory Page" template. These are not Logseq limits or features, and the only size limit in source is 100 MiB per file.

---

## Code References

- [`src/main/frontend/handler/editor.cljs`](../../../../../src/main/frontend/handler/editor.cljs): `new-asset-block`, `db-based-save-assets!`, `upload-asset!`, `delete-asset-of-block!`, `resize-image!`
- [`src/main/frontend/handler/assets.cljs`](../../../../../src/main/frontend/handler/assets.cljs): `exceed-limit-size?`, `get-file-checksum`, `<make-asset-url`, `<get-all-assets`
- [`src/main/frontend/components/block.cljs`](../../../../../src/main/frontend/components/block.cljs): `asset-container`, `resizable-image`, `asset-link`, `asset-cp`
- [`src/main/frontend/worker/sync/assets.cljs`](../../../../../src/main/frontend/worker/sync/assets.cljs): `upload-remote-asset!`, `download-remote-assets-if-missing!`
- [`deps/db/src/logseq/db/frontend/class.cljs`](../../../../../deps/db/src/logseq/db/frontend/class.cljs): `:logseq.class/Asset`
- [`deps/db/src/logseq/db/frontend/property.cljs`](../../../../../deps/db/src/logseq/db/frontend/property.cljs): `:logseq.property.asset/*`
- [`deps/db/src/logseq/db/frontend/asset.cljs`](../../../../../deps/db/src/logseq/db/frontend/asset.cljs): checksum, type and title helpers

---

## Related Documentation

- [Assets: Link Syntax, File Types, and Sync](../../reference/features/asset-sync.md): every link form, format sets, sync eligibility
- [Sync Limits](../../reference/features/sync-limits.md): size limits in full
- [Video Embedding](../multimedia/video-embed.md): YouTube, Vimeo, Bilibili, video URLs
- [PDF Annotations](../../reference/features/pdf-annotations.md): working with PDF files
- [Backup and Recovery Strategies](../organization/backup-strategies.md): exports that include assets
- [Schema Overview](../../reference/database/schema-overview.md): classes and properties
- [E2EE Encryption](../../explanation/security/e2ee-encryption.md): what is encrypted in synced graphs
- [Basic Editing](../../tutorials/getting-started/03-basic-editing.md)
