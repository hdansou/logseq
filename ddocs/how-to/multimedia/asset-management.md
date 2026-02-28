---
title: 'Asset Management in Logseq'
status: '🟢 Stable'
version: 'Since v0.5.0'
quadrant: 'How-To Guide'
platform: 'Desktop (macOS/Windows/Linux), Mobile (iOS/Android)'
related-docs:
  - '../multimedia/video-embed.md'
  - '../../reference/database/schema-overview.md'
  - '../../tutorials/getting-started/03-basic-editing.md'
---

# Asset Management in Logseq

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

**Code Reference**: [`assets.cljs`](../../../src/main/frontend/handler/assets.cljs)

### Database Graphs

**Storage Structure**:

Assets stored as `:logseq.class/Asset` blocks with properties:

```clojure
{:block/uuid "12345678-1234-1234-1234-123456789abc"
 :block/tags [:logseq.class/Asset]
 :logseq.property.asset/type "image/png"
 :logseq.property.asset/size 524288  ; bytes
 :logseq.property.asset/checksum "sha256:abcd..."
 :logseq.property.asset/path "../assets/image_1234.png"
 :logseq.property.asset/created-at 1705852800000}
```

**Asset Class Definition**: [`class.cljs:57-63`](../../../deps/db/src/logseq/db/frontend/class.cljs#L57-L63)

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

- `:logseq.property.asset/type` — MIME type (e.g., `"image/png"`, `"application/pdf"`)
- `:logseq.property.asset/size` — File size in bytes
- `:logseq.property.asset/checksum` — SHA-256 hash for integrity verification
- `:logseq.property.asset/path` — Relative path in graph
- `:logseq.property.asset/created-at` — Upload timestamp
- `:logseq.property.asset/external-url` — Original URL for web links

### Remote Graphs (RTC Sync)

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

**Code Reference**: [`assets.cljs:234-262`](../../../src/main/frontend/handler/assets.cljs#L234-L262)

```clojure
(defn new-task--rtc-upload-asset
  [repo aes-key asset-block-uuid-str asset-type checksum put-url]
  (m/sp
    (let [r (m/? (http/put put-url
                   {:body (get-asset-file-arraybuffer)
                    :with-credentials? false
                    :progress-chan (c.m/run-task :upload-asset-progress
                                     (upload-progress-flow asset-block-uuid-str))}))
          success? (http/success? r)]
      (if success?
        {:ok {:asset-block-uuid-str asset-block-uuid-str}}
        (throw (ex-info "upload-asset failed"
                        {:type :rtc.exception/upload-asset-failed
                         :data (dissoc r :body)}))))))
```

**Download Flow**: Similar but reversed (GET request, decrypt, write to local assets).

**Size Limit**: 100 MB per asset (see [`assets.cljs:21-24`](../../../src/main/frontend/handler/assets.cljs#L21-L24)):

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

**Code Reference**: [`assets.cljs:26-62`](../../../src/main/frontend/handler/assets.cljs#L26-L62)

```clojure
(defn alias-enabled? []
  (and (util/electron?)
       (:assets/alias-enabled? @state/state)))

(defn get-alias-by-name [name]
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
 :logseq.property.asset/type "image/png"
 ;; checksum and size from initial download
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
             (property :logseq.property.asset/type "image/png"))}}
```

**Result**: All PNG images in graph

**Get Asset Size**:

```clojure
{{query (property-sum :logseq.property.asset/size)}}
```

**Result**: Total size of all assets in bytes

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

**Cause**: Logseq appends timestamp to prevent collisions

**Solution**:

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

- [`src/main/frontend/handler/assets.cljs`](../../../src/main/frontend/handler/assets.cljs) (345 lines) — Asset upload/download logic
- [`deps/db/src/logseq/db/frontend/class.cljs`](../../../deps/db/src/logseq/db/frontend/class.cljs#L57-L63) — Asset class definition
- [`deps/db/src/logseq/db/frontend/asset.cljs`](../../../deps/db/src/logseq/db/frontend/asset.cljs) — Database asset utilities

**Key Functions**:

- `new-task--rtc-upload-asset` (line 234-262) — RTC upload with encryption
- `new-task--rtc-download-asset` (line 264-283) — RTC download and decryption
- `exceed-limit-size?` (line 21-24) — 100 MB size limit check
- `normalize-asset-resource-url` (line 92-108) — Path to URL conversion
- `resolve-asset-real-path-url` (line 64-88) — Resolve asset paths

---

## Related Documentation

**Tutorials**:

- [Basic Editing](../../tutorials/getting-started/03-basic-editing.md) — Learn markdown syntax for asset embedding

**How-To Guides**:

- [Video Embedding](../multimedia/video-embed.md) — YouTube, Vimeo, local videos
- [PDF Annotations](../../reference/features/pdf-annotations.md) — Working with PDF files

**References**:

- [Schema Overview](../../reference/database/schema-overview.md) — Database schema overview
- [Properties Reference](../../reference/data/properties-reference.md) — Asset-related properties reference

**Explanations**:

- [E2EE Encryption](../../explanation/security/e2ee-encryption.md) — How assets are encrypted in remote graphs
- [RTC Sync](../../tutorials/collaboration/real-time-sync.md) — Asset synchronization in collaboration

---

**Last Updated**: January 2026  
**Status**: 🟢 Stable (Asset management core feature)  
**Feedback**: Report asset issues on [GitHub](https://github.com/logseq/logseq/issues) with `assets` label
