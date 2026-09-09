---
title: 'Asset Management in Logseq'
status: '🟢 Stable'
version: 'Since v0.5.0'
quadrant: 'How-To Guide'
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
verifiedScope: 'Remote Graphs (RTC Sync) section only'
platform: 'Desktop (macOS/Windows/Linux), Mobile (iOS/Android)'
related-docs:
  - '../../reference/features/asset-sync.md'
  - '../multimedia/video-embed.md'
  - '../../reference/database/schema-overview.md'
  - '../../tutorials/getting-started/03-basic-editing.md'
---

# Asset Management in Logseq

> **Last revised:** 2026-05-30

## Overview

Assets are files embedded in a graph: images, PDFs, videos, audio, and other documents. The current implementation resolves asset links by file type and platform, and it supports both file-based graphs and DB graphs.

**Common entry points**:

- Drag and drop a file into a block.
- Paste an image from the clipboard.
- Use the asset upload command from the editor UI.

## Findings: New Capabilities

- Asset links now resolve inline images, inline audio/video, PDF viewer links, and generic clickable file links from the same rendering path.
- Desktop asset aliases are supported through Settings > Assets, with `@alias` directories mapped to real folders.
- Remote asset sync writes asset bytes through db-sync handlers and records `:logseq.property.asset/remote-metadata` after a successful upload; encryption only applies when E2EE is enabled.

## Asset Rendering

The current block renderer chooses the presentation from the resolved extension.

- Images render inline.
- PDFs open the PDF viewer.
- Audio and video use inline media elements when the extension matches the configured media lists.
- Other files render as clickable asset links.

> **Removed:** the earlier blanket claim that every asset behaves like an inline preview. The renderer is extension-based, and non-media assets remain plain links.

## Storage Model

### File-Based Graphs

Files live under the graph's `assets/` directory.

**Typical layout**:

```text
/path/to/graph/
  ├── pages/
  ├── assets/
  │   ├── image-1_1234567890.png
  │   ├── document_abcdef.pdf
  │   └── video-demo_ghijkl.mp4
  └── logseq/
```

### DB Graphs

DB graphs represent assets as `:logseq.class/Asset` blocks. The current class schema requires only these properties:

```clojure
{:block/uuid "12345678-1234-1234-1234-123456789abc"
 :block/tags [:logseq.class/Asset]
 :logseq.property.asset/type "png"
 :logseq.property.asset/size 524288
 :logseq.property.asset/checksum "sha256:abcd..."}
```

Additional asset-related properties exist in the built-in property registry and are used by rendering or sync flows:

- `:logseq.property.asset/external-url`
- `:logseq.property.asset/external-file-name`
- `:logseq.property.asset/width`
- `:logseq.property.asset/height`
- `:logseq.property.asset/remote-metadata`
- `:logseq.property.asset/last-visit-page`
- `:logseq.property.asset/resize-metadata`

> **Removed:** the older example that showed `:logseq.property.asset/path` and `:logseq.property.asset/created-at` is not aligned with the current asset class schema.

## Upload and Sync

### Size Limit

Uploads are capped at 100 MB per asset in both the frontend and db-sync code paths.

### Remote Graph Sync

When db-sync is enabled, the current flow reads the local asset bytes, optionally encrypts them if the graph uses E2EE, and uploads them to the sync bucket with the graph id, asset UUID, and file type in the object path.

After a successful upload, the asset block is updated with remote metadata so the graph knows the asset has a synced remote copy.

> **Removed:** the earlier description of remote graphs as a single generic "encrypted cloud storage" feature was too broad. The implementation is specifically asset-op driven and only encrypts when E2EE is on.

## Asset Aliases

Asset aliases are a desktop-only feature.

1. Open Settings > Assets.
2. Enable Asset Aliases.
3. Add a directory and assign it an alias name.

The renderer can then resolve `@alias` paths to the configured directory.

## Troubleshooting

- If an asset breaks, first check the file still exists in `assets/` and the filename matches exactly.
- If a remote asset does not load on another device, check whether the asset block has `:logseq.property.asset/remote-metadata` and whether the graph is syncing.
- If a PDF does not open in the viewer, confirm the extension is actually `.pdf` and the file is not corrupted.

## Best Practices

- Prefer relative asset links over absolute filesystem paths.
- Keep large media out of the graph when an external URL is acceptable.
- Use aliases for shared desktop-only asset directories.
- Keep filenames stable after publishing or sharing a graph.

## Code References

- [src/main/frontend/components/block.cljs](../../../../../src/main/frontend/components/block.cljs) - current asset rendering logic
- [src/main/frontend/handler/assets.cljs](../../../../../src/main/frontend/handler/assets.cljs) - asset path resolution and size checks
- [src/main/frontend/worker/sync/assets.cljs](../../../../../src/main/frontend/worker/sync/assets.cljs) - db-sync asset upload and remote metadata updates
- [deps/db/src/logseq/db/frontend/class.cljs](../../../../../deps/db/src/logseq/db/frontend/class.cljs) - current `:logseq.class/Asset` schema
- [deps/db/src/logseq/db/frontend/property.cljs](../../../../../deps/db/src/logseq/db/frontend/property.cljs) - built-in asset properties
- [deps/db/src/logseq/db/frontend/asset.cljs](../../../../../deps/db/src/logseq/db/frontend/asset.cljs) - checksum and asset utility helpers

---
title: 'Asset Management in Logseq'
status: '🟢 Stable'
version: 'Since v0.5.0'
quadrant: 'How-To Guide'
platform: 'Desktop (macOS/Windows/Linux), Mobile (iOS/Android)'
related-docs:
  - '../multimedia/video-embed.md'
  - '../../reference/schema/built-in-classes.md#logseqclassasset'
  - '../../tutorials/getting-started/basic-editing.md'
---

# Asset Management in Logseq

> **Last revised:** 2026-03-10

## Overview

Assets are files embedded in your Logseq graph — images, PDFs, videos, audio, and other documents. Logseq provides comprehensive asset management features including upload, storage, synchronization, and organization.

**Asset Types**:

- 🖼️ Images (PNG, JPG, GIF, SVG, WebP)
- 📄 PDFs (with annotation support)
- 🎥 Videos (MP4, WebM, MOV)
- 🎵 Audio (MP3, WAV, OGG)
- 📎 Files (any file type)

**Storage**:

- **File-based graphs**: `assets/` folder in graph directory
- **Database graphs**: `:logseq.class/Asset` blocks with metadata
- **Remote graphs**: Encrypted cloud storage with RTC sync

## Quick Start

### Upload an Image

**Drag & Drop**:

1. Drag image file from file manager
2. Drop onto any block
3. Image automatically uploaded to `assets/`
4. Markdown link inserted: `![](../assets/image.png)`

**Copy-Paste**:

1. Copy image from clipboard (screenshot, web, etc.)
2. Click into a block
3. Paste (Cmd+V / Ctrl+V)
4. Image uploaded automatically

**File Picker**:

1. Type `/Upload an image`
2. Select file from dialog
3. Image inserted

---

## Adding Assets

### Images

**Markdown Syntax**:

```markdown
![Image description](../assets/my-image.png)
```

**With Properties**:

```markdown
![Diagram](../assets/diagram.png){:width 600 :height 400}
```

**External Image URL**:

```markdown
![Logo](https://example.com/logo.png)
```

**Result**: Rendered image inline in block

### PDFs

**PDF Link**:

```markdown
![Document](../assets/report.pdf)
```

**Result**: Opens PDF viewer with annotation tools (desktop only)

**See**: [PDF Reference Documentation](../../reference/features/pdf-annotations.md)

### Videos

**Local Video**:

```markdown
![Demo](../assets/demo.mp4)
```

**YouTube**:

```markdown
{{video https://www.youtube.com/watch?v=VIDEO_ID}}
```

**See**: [Video Embedding Guide](../multimedia/video-embed.md)

### Audio Files

**Audio Link**:

```markdown
![Podcast Episode](../assets/podcast.mp3)
```

**Result**: Audio player with controls

### Generic Files

**Any File Type**:

```markdown
![Spreadsheet](../assets/data.xlsx)
```

**Result**: Download link (if browser can't display)

---

## Asset Storage

### File-Based Graphs

**Storage Location**:

```
/path/to/graph/
  ├── pages/
  ├── assets/
  │   ├── image-1_1234567890.png
  │   ├── document_abcdef.pdf
  │   └── video-demo_ghijkl.mp4
  └── logseq/
```

**File Naming**:

- Original name: `my-image.png`
- Stored as: `my-image_<timestamp>.png`
- Timestamp prevents name collisions
- Spaces replaced with hyphens

**Code Reference**: [`assets.cljs`](../../src/main/frontend/handler/assets.cljs)

### Database Graphs

**Storage Structure**:

Assets stored as `:logseq.class/Asset` blocks with properties:

```clojure
{:block/uuid "12345678-1234-1234-1234-123456789abc"
 :block/tags [:logseq.class/Asset]
 :logseq.property.asset/type "png"
 :logseq.property.asset/size 524288  ; bytes
 :logseq.property.asset/checksum "abcd1234..."  ; SHA-256 hex digest
 :logseq.property.asset/external-url nil}       ; set for web-linked assets
;; File stored on disk as: assets/<block-uuid>.png
```

**Asset Class Definition**: [`class.cljs:58-64`](../../deps/db/src/logseq/db/frontend/class.cljs#L58-L64)

```clojure
:logseq.class/Asset
{:title "Asset"
 :properties {:logseq.property.class/hide-from-node true
              :logseq.property.view/type :logseq.property.view/type.gallery}
 :schema {:properties [:logseq.property.asset/type
                       :logseq.property.asset/size
                       :logseq.property.asset/checksum]
          :required-properties [:logseq.property.asset/type
                                :logseq.property.asset/size
                                :logseq.property.asset/checksum]}}
```

**Properties**:

- `:logseq.property.asset/type` — File extension string (e.g., `"png"`, `"pdf"`, `"mp4"`)
- `:logseq.property.asset/size` — File size in bytes
- `:logseq.property.asset/checksum` — SHA-256 hex digest for integrity verification
- `:logseq.property.asset/external-url` — Original URL for web-linked assets
- `:logseq.property.asset/external-file-name` — Original filename for externally sourced assets
- `:logseq.property.asset/width` — Image width in pixels (set automatically on load)
- `:logseq.property.asset/height` — Image height in pixels (set automatically on load)
- `:logseq.property.asset/remote-metadata` — Metadata for assets stored in remote/cloud storage
- `:logseq.property.asset/last-visit-page` — Last visited page number (for PDFs)
- `:logseq.property.asset/resize-metadata` — Resize dimensions metadata

### Remote Graphs (RTC Sync)

> Sync replicates **every** file type — there is no extension allowlist. What it will
> not do is pick up a file copied into `assets/` by hand: sync reads asset *entities*,
> never the directory. See
> [Assets: Link Syntax, File Types, and Sync](../../reference/features/asset-sync.md).

**Encrypted Upload**:

1. Asset uploaded to local `assets/` folder
2. File encrypted with AES-256-GCM (if E2EE enabled)
3. Encrypted blob uploaded to cloud
4. Other collaborators download and decrypt automatically

**Upload Flow**:

```txt
Local File
    ↓
Read as ArrayBuffer
    ↓
Encrypt with AES-256 (if E2EE)
    ↓
Upload to Logseq Cloud (via PUT request)
    ↓
Server stores encrypted blob
    ↓
Broadcast asset metadata to collaborators
    ↓
Collaborators download on-demand
    ↓
Decrypt and save to local assets/
```

**Code Reference**: [`assets.cljs:301-334`](../../src/main/frontend/handler/assets.cljs#L301-L334)

```clojure
(defn new-task--rtc-upload-asset
  [repo aes-key asset-block-uuid-str asset-type checksum put-url & {:keys [extra-headers]}]
  (assert (and asset-type checksum))
  (m/sp
    (let [asset-file (try (c.m/<? (<read-asset repo asset-block-uuid-str asset-type))
                          (catch :default e
                            (log/info :read-asset e)
                            (throw (ex-info "read-asset failed" {:type :rtc.exception/read-asset-failed} e))))
          asset-file (if aes-key (->uint8 asset-file) asset-file)
          asset-file* (if (not aes-key)
                        asset-file
                        (ldb/write-transit-str
                         (c.m/<? (crypt/<encrypt-uint8array aes-key asset-file))))
          ;; ... progress tracking and HTTP PUT ...
          ]
      ;; Throws :rtc.exception/upload-asset-failed on non-2xx status
      )))
```

**Download Flow**: Similar but reversed (GET request, decrypt, write to local assets).

**Size Limit**: 100 MB per asset (see [`assets.cljs:22-25`](../../src/main/frontend/handler/assets.cljs#L22-L25)):

```clojure
(defn exceed-limit-size?
  "Asset size no more than 100M"
  [^js file]
  (> (.-size file) (* 100 1024 1024)))
```

---

## Organizing Assets

### Folder Structure (File-Based)

**Manual Organization** (advanced):

```
assets/
  ├── images/
  │   ├── screenshots/
  │   ├── diagrams/
  │   └── photos/
  ├── documents/
  │   ├── pdfs/
  │   └── spreadsheets/
  └── media/
      ├── videos/
      └── audio/
```

**Reference in Markdown**:

```markdown
![Architecture](../assets/diagrams/architecture.png)
```

⚠️ **Warning**: Logseq doesn't automatically create subfolders. You must organize manually and update links accordingly.

### Asset Aliases (Desktop Only)

**Setup**:

1. Open Settings → Assets
2. Enable "Asset Aliases"
3. Add alias directory:
   - Name: `@shared`
   - Path: `/path/to/shared/assets`

**Usage**:

```markdown
![Company Logo](assets/@shared/logo.png)
```

**Benefits**:

- Share assets across multiple graphs
- Keep assets outside graph folder
- Useful for team templates, branding

**Code Reference**: [`assets.cljs:27-58`](../../src/main/frontend/handler/assets.cljs#L27-L58)

```clojure
(defn alias-enabled?
  []
  (and (util/electron?)
       (:assets/alias-enabled? @state/state)))

(defn get-alias-by-name
  [name]
  (when-let [alias-dirs (and (alias-enabled?) (seq (get-alias-dirs)))]
    (medley/find-first #(= name (:name (second %1)))
                       (medley/indexed alias-dirs))))
```

### Tags for Asset Discovery

**Tag Assets**:

```markdown
- [[Assets/Screenshots]]
  - ![Screenshot 1](../assets/screen1.png) #ui #bug
  - ![Screenshot 2](../assets/screen2.png) #ui #feature
```

**Query All Images with Tag**:

```clojure
{{query (and [[Assets]] #ui)}}
```

**Result**: All asset blocks tagged `#ui`

---

## Asset Operations

### Viewing Assets

**Images**: Rendered inline automatically

**Click to Zoom**: Click image → Opens lightbox with:

- Full-size view
- Zoom controls (+ / -)
- Close button (X or Esc)

**PDFs**: Click → Opens PDF viewer (desktop only)

**Videos**: Rendered as inline player

**Other Files**: Click → Downloads file

### Copying Asset Links

1. Hover over asset link
2. Right-click
3. Select "Copy image link" or "Copy asset link"
4. Paste elsewhere: `![](../assets/file.png)`

### Renaming Assets

**File-Based**:

1. Rename file in `assets/` folder manually
2. Update references in graph:

   ```markdown
   <!-- Old -->

   ![](../assets/old-name_1234.png)

   <!-- New -->

   ![](../assets/new-name_1234.png)
   ```

**Database**: Rename managed by Logseq automatically (asset UUID-based)

### Deleting Assets

**Remove Reference**:

- Delete markdown link from block
- Asset file remains in `assets/` folder

**Delete File**:

**File-Based**:

1. Delete file from `assets/` folder manually
2. Broken image icon appears in blocks referencing it

**Database**:

1. Delete asset block from database
2. File removed from `assets/` folder automatically
3. References show broken link

**Cleanup Unused Assets** (🟡 Planned):

- Future feature: Scan for unreferenced assets
- Suggest deletion to free up space

### Moving Assets Between Graphs

**Method 1: Copy-Paste**

1. Open asset in source graph
2. Copy image (right-click → Copy image)
3. Switch to target graph
4. Paste into block

**Method 2: Manual File Copy**

1. Copy asset file from `source/assets/` to `target/assets/`
2. Update file name if timestamp differs
3. Create reference: `![](../assets/file.png)`

**Method 3: Export/Import**

- Export page with assets (Settings → Export)
- Import into target graph
- Assets included automatically

---

## Advanced Features

### External Asset URLs

**Link to Web Images**:

```markdown
![GitHub Logo](https://github.githubassets.com/images/modules/logos_page/GitHub-Mark.png)
```

**Pros**:

- No local storage used
- Always up-to-date (if source updates)
- Shareable links

**Cons**:

- Requires internet connection
- Broken if source URL changes
- Not included in graph backups

**Storage in Database**:

```clojure
{:logseq.property.asset/external-url "https://example.com/image.png"
 :logseq.property.asset/type "png"
 ;; checksum and size computed from initial download
 }
```

### Data URLs (Embedded Images)

**Embed Small Images Directly**:

```markdown
![](data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAUA...)
```

**Use Cases**:

- Tiny icons (< 1 KB)
- SVG graphics
- Generated QR codes

**Limitation**: Large data URLs make markdown files huge (avoid for > 10 KB).

### Asset Metadata

**Query Asset Properties** (database graphs):

```clojure
{{query (and [[logseq.class/Asset]]
             (property :logseq.property.asset/type "png"))}}
```

**Result**: All PNG images in graph

### Batch Asset Operations (🟡 Planned)

Future features:

- Select multiple assets
- Bulk delete/move/tag
- Convert image formats
- Compress images

---

## Platform-Specific Notes

### Desktop (Electron)

**Features**:

- ✅ Full asset management
- ✅ Asset aliases supported
- ✅ PDF annotations
- ✅ Drag & drop from file manager
- ✅ Paste from clipboard
- ✅ External windows for PDFs

**Best Experience**: Desktop has all asset features.

### Mobile (iOS/Android)

**Features**:

- ✅ Image upload (camera or photo library)
- ✅ Image viewing with zoom
- ⚠️ PDF viewing limited (no annotations)
- ⚠️ Video playback (codec-dependent)
- ❌ Asset aliases not supported
- ❌ External windows not available

**Photo Integration**:

- Take photo → Insert directly into block
- Choose from library → Upload to assets

### Web Browser

**Features**:

- ✅ Image upload and viewing
- ✅ External URL assets
- ⚠️ Local file access restricted (security)
- ⚠️ PDF viewer may have limitations
- ❌ Asset aliases not available

**CORS Issues**: Some external URLs may be blocked by browser security policies.

---

## Troubleshooting

### Asset Not Displaying

**Problem**: Broken image icon instead of image

**Possible Causes**:

1. File moved or deleted
2. Incorrect file path
3. File name mismatch (case-sensitive on Linux/macOS)
4. CORS blocking external URL

**Solutions**:

- Check file exists in `assets/` folder
- Verify path: `../assets/filename.ext` (relative to page)
- Check browser console for errors (Help → Toggle Developer Tools)
- For external URLs: test in browser first

### Upload Fails

**Problem**: Drag & drop doesn't work

**Possible Causes**:

1. File size exceeds 100 MB limit
2. Network error (remote graphs)
3. Disk space full
4. Permission denied (file system access)

**Solutions**:

- Check file size (compress if necessary)
- Check internet connection (for RTC)
- Free up disk space
- Check folder permissions (desktop)

### Asset Path Issues

**Problem**: Asset works locally but broken on another device

**Possible Causes**:

1. Using absolute path instead of relative
2. Asset not synced (file-based graphs)
3. Different file systems (Windows vs macOS)

**Solutions**:

- Use relative paths: `../assets/file.png` (not `/Users/name/...`)
- Ensure `assets/` folder synced via Git/Dropbox
- Check file name case (Windows case-insensitive, macOS/Linux case-sensitive)

### Duplicate Assets

**Problem**: Same file uploaded multiple times with different names

**Cause**: In file-based graphs, Logseq appends a timestamp to prevent collisions. In database graphs, Logseq deduplicates by SHA-256 checksum and will warn if an identical asset already exists.

**Solution** (file-based graphs):

- Search graph for duplicate references
- Consolidate to one asset file
- Update all references to point to single file
- Delete duplicates manually

---

## Best Practices

### Performance

✅ **Do**:

- Compress images before upload (< 500 KB per image)
- Use JPEG for photos, PNG for graphics
- Use external URLs for large files when possible
- Keep total assets < 5 GB per graph

❌ **Don't**:

- Upload uncompressed 4K images (10+ MB each)
- Embed videos > 50 MB (use YouTube instead)
- Store hundreds of PDFs locally (link externally)

**Image Compression Tools**:

- **ImageOptim** (Mac): Lossless compression
- **TinyPNG** (Web): Up to 70% size reduction
- **ffmpeg** (CLI): Batch processing

```bash
# Compress JPEG to 80% quality
ffmpeg -i input.jpg -q:v 5 output.jpg

# Resize to max width 1200px
ffmpeg -i input.png -vf scale=1200:-1 output.png
```

### Organization

**Consistent Naming**:

```
assets/
  ├── 2024-01-20_meeting-screenshot.png
  ├── 2024-01-20_whiteboard-diagram.png
  ├── project-x_architecture-v2.png
```

**Use Prefixes**:

- Date prefix for chronological: `2024-01-20_`
- Project prefix for grouping: `projectx_`
- Type suffix: `_diagram`, `_screenshot`, `_photo`

**Asset Inventory Page**:

```markdown
- [[📎 Asset Inventory]]
  - Last audit: [[2024-01-20]]
  - Total assets: 324
  - Total size: 2.4 GB
  - Categories:
    - Screenshots: 156 files
    - Diagrams: 87 files
    - Documents: 81 files
```

### Backups

**Include Assets in Backups**:

- Git: Commit `assets/` folder (if < 1 GB)
- Export: Settings → Export graph → Includes assets
- Manual: Copy entire graph folder

**Exclude Large Assets from Git**:

```gitignore
# .gitignore
assets/*.mp4
assets/*.mov
assets/*.zip
```

**External Asset Archive**:

- Store large files outside graph
- Link externally: Dropbox, Google Drive, S3
- Keep inventory page with links

---

## Code Reference

**Core Implementation**:

- [`src/main/frontend/handler/assets.cljs`](../../src/main/frontend/handler/assets.cljs) (~418 lines) — Asset upload/download logic
- [`deps/db/src/logseq/db/frontend/class.cljs`](../../deps/db/src/logseq/db/frontend/class.cljs#L58-L64) — Asset class definition
- [`deps/db/src/logseq/db/frontend/asset.cljs`](../../deps/db/src/logseq/db/frontend/asset.cljs) — Database asset utilities

**Key Functions**:

- `new-task--rtc-upload-asset` (line 301-334) — RTC upload with encryption
- `new-task--rtc-download-asset` (line 336-374) — RTC download and decryption
- `exceed-limit-size?` (line 22-25) — 100 MB size limit check
- `normalize-asset-resource-url` (line 85-102) — Path to URL conversion
- `resolve-asset-real-path-url` (line 67-83) — Resolve asset paths

---

## Related Documentation

**Tutorials**:

- [Basic Editing](../../tutorials/getting-started/basic-editing.md) — Learn markdown syntax for asset embedding

**How-To Guides**:

- [Video Embedding](../multimedia/video-embed.md) — YouTube, Vimeo, local videos
- [PDF Annotations](../multimedia/pdf-annotations.md) — Working with PDF files

**References**:

- [Built-in Classes](../../reference/schema/built-in-classes.md#logseqclassasset) — Asset class schema
- [Built-in Properties](../../reference/schema/built-in-properties.md#asset-properties) — Asset properties reference

**Explanations**:

- [E2EE Encryption](../../explanation/security/e2ee-encryption.md) — How assets are encrypted in remote graphs
- [RTC Sync](../../tutorials/collaboration/real-time-sync.md) — Asset synchronization in collaboration

---

**Last Updated**: March 2026
**Status**: 🟢 Stable (Asset management core feature)  
**Feedback**: Report asset issues on [GitHub](https://github.com/logseq/logseq/issues) with `assets` label
