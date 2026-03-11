---
title: 'Video Embedding in Logseq'
status: '🟢 Stable'
version: 'Since v0.6.0'
quadrant: 'How-To Guide'
platform: 'Desktop (macOS/Windows/Linux), Web'
related-docs:
  - '../multimedia/asset-management.md'
  - '../../reference/syntax/markdown-extensions.md'
  - '../../tutorials/getting-started/basic-editing.md'
---

# Video Embedding in Logseq

> **Last revised:** 2026-03-10

## Overview

Logseq supports embedding videos from YouTube, Vimeo, Bilibili, Loom, and other platforms, as well as local video files. Videos are embedded as interactive players directly in your blocks, allowing you to watch content without leaving your graph.

**Supported Sources**:

- YouTube (with timestamp support and API control)
- Vimeo
- Bilibili (Chinese platform)
- Loom
- YouTube-nocookie (privacy-enhanced mode)
- Local video files (MP4, WebM, MOV, FLV, AVI, MKV)

## Quick Start

### YouTube Video

**Syntax**:

```markdown
{{video https://www.youtube.com/watch?v=VIDEO_ID}}
```

**Example**:

```markdown
{{video https://www.youtube.com/watch?v=dQw4w9WgXcQ}}
```

**Result**: Embedded YouTube player (560x315px by default)

---

### Local Video File

**Syntax**:

```markdown
![Video](../assets/my-video.mp4)
```

**Example**:

```markdown
![Project Demo](../assets/demo-2024.mp4)
```

**Result**: Native HTML5 video player with controls

---

## YouTube Embedding

### Basic YouTube Embed

**Full URL**:

```markdown
{{video https://www.youtube.com/watch?v=dQw4w9WgXcQ}}
```

**Short URL**:

```markdown
{{video https://youtu.be/dQw4w9WgXcQ}}
```

**With Parameters**:

```markdown
{{youtube https://www.youtube.com/watch?v=dQw4w9WgXcQ}}
```

All three syntaxes work identically and embed the same player.

### YouTube with Timestamps

**Start at Specific Time**:

```markdown
{{video https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=90s}}
```

**Parameters**:

- `t=90` — Start at 90 seconds (numeric seconds only)

Note: The `&t=` parameter accepts numeric seconds only (e.g., `t=90`). Formats like `t=1m30s` are not parsed by Logseq; convert to total seconds instead.

**Result**: Player starts automatically at specified timestamp

### Custom Player Size

**Using Width Parameter** (🟡 Advanced):

While not directly supported in macro syntax, you can control size via CSS:

```markdown
{{video https://www.youtube.com/watch?v=dQw4w9WgXcQ}}
```

Then add custom CSS:

```css
/* In custom.css — target by iframe id */
#youtube-player-dQw4w9WgXcQ {
  width: 800px;
  height: 450px;
}
```

**Default Sizes**:

- Width: Adapts to container (max 560px)
- Height: Maintains 16:9 aspect ratio (315px for 560px width)
- Mobile: Full width of screen minus padding

**Code Reference**: [`youtube.cljs:64-66`](../../src/main/frontend/extensions/video/youtube.cljs#L64-L66)

```clojure
(let [width  (or width (min (- (util/get-width) 96) 560))
      height (or height (int (* width (/ 315 560))))]
  ;; Creates iframe with calculated dimensions
  )
```

### YouTube Timestamp Links

**Creating Clickable Timestamps**:

```markdown
- Watch intro at {{youtube-timestamp 0:30}}
- Jump to main section at {{youtube-timestamp 2:45}}
- See conclusion at {{youtube-timestamp 10:15}}
```

**Behavior**:

- Clicking timestamp seeks video to that point
- Works with most recently played YouTube video on page
- Displays as formatted timestamp (e.g., "2:45")

**Mobile Limitation**: Timestamp controls don't work on iOS/Android native apps (WebView limitation).

**Code Reference**: [`youtube.cljs:139-154`](../../src/main/frontend/extensions/video/youtube.cljs#L139-L154)

```clojure
(rum/defc timestamp
  [seconds]
  [:a.svg-small.youtube-timestamp
   {:on-click (fn [e]
                (util/stop e)
                (if (use-youtube-wrapper?)
                  (notify-timestamp-unavailable!)
                  (when-let [player (get-player (.-target e))]
                    (if-let [seek-to (player-method player "seekTo")]
                      (.call seek-to player seconds true)
                      (notification/show!
                       "YouTube player isn't ready yet."
                       :warning
                       false)))))}
   svg/clock
   (seconds->display seconds)])
```

### YouTube API Integration

**Desktop/Web Only**: Logseq uses YouTube IFrame API for advanced controls.

**Features Enabled**:

- Programmatic playback control
- Timestamp seeking
- Event handling (ready, state changes)
- Multiple players on same page

**Code Reference**: [`youtube.cljs:20-27`](../../src/main/frontend/extensions/video/youtube.cljs#L20-L27)

```clojure
(defn load-youtube-api []
  (let [c (chan)]
    (if js/window.YT
      (a/close! c)
      (do
        (set! js/window.onYouTubeIframeAPIReady #(a/close! c))
        (load-yt-script)))
    c))
```

**Mobile Limitation**: Native iOS/Android apps use wrapper HTML page instead of direct API due to platform restrictions.

---

## Local Video Files

### Supported Formats

Logseq supports standard HTML5 video formats:

| Format          | Extension | Browser Support    | Notes                                |
| --------------- | --------- | ------------------ | ------------------------------------ |
| MP4 (H.264)     | `.mp4`    | All browsers       | **Recommended** — Best compatibility |
| WebM (VP8/VP9)  | `.webm`   | Most browsers      | Good compression, open format        |
| MOV (QuickTime) | `.mov`    | Safari mainly      | Apple format, limited support        |
| FLV (Flash)     | `.flv`    | Limited            | Legacy format, needs plugin/codec    |
| AVI             | `.avi`    | Varies             | Legacy format, avoid if possible     |
| MKV (Matroska)  | `.mkv`    | Limited            | Container format, limited browser support |

**Recommendation**: Use **MP4 with H.264 codec** for maximum compatibility.

**Code Reference**: [`config.cljs`](../../src/main/frontend/config.cljs) defines supported formats:

```clojure
(def video-formats
  #{:mp4 :webm :mov :flv :avi :mkv})
```

### Adding Local Videos

**Step 1: Copy Video to Assets Folder**

```bash
# File-based graphs
cp my-video.mp4 /path/to/graph/assets/

# Database graphs
# Use Logseq's "Upload" feature or drag & drop
```

**Step 2: Embed in Block**

```markdown
![Demo Video](../assets/my-video.mp4)
```

Or using image macro:

```markdown
{{video ../assets/my-video.mp4}}
```

**Result**: Native HTML5 `<video>` element with controls

### Video Player Controls

**Native HTML5 Controls**:

- ▶️ Play/Pause
- 🔊 Volume control
- ⏩ Seek bar
- ⏱️ Current time / Duration
- ⛶ Fullscreen (desktop only)
- ⚙️ Playback speed (browser-dependent)

**Keyboard Shortcuts** (browser-dependent):

- `Space` — Play/Pause
- `←` / `→` — Seek backward/forward 5 seconds
- `↑` / `↓` — Volume up/down
- `F` — Fullscreen
- `M` — Mute/Unmute

### Video Properties

**Set Video Width**:

```markdown
![](../assets/video.mp4){:width 800}
```

**Set Video Height**:

```markdown
![](../assets/video.mp4){:height 450}
```

**Both Dimensions**:

```markdown
![](../assets/video.mp4){:width 800 :height 450}
```

**Aspect Ratio**: If only width or height is specified, browser maintains original aspect ratio.

---

## Other Platforms

### Vimeo

**Syntax** (either form works):

```markdown
{{video https://vimeo.com/VIDEO_ID}}
{{vimeo https://vimeo.com/VIDEO_ID}}
```

**Example**:

```markdown
{{video https://vimeo.com/123456789}}
```

**Result**: Embedded Vimeo player (uses Vimeo's embed URL)

### Loom

**Syntax**:

```markdown
{{video https://www.loom.com/share/VIDEO_ID}}
```

**Result**: Embedded Loom player

### Bilibili (Chinese Platform)

**Syntax** (either form works):

```markdown
{{video https://www.bilibili.com/video/BV1XXXXXXXXX}}
{{bilibili https://www.bilibili.com/video/BV1GJ411x7h7}}
```

**Result**: Embedded Bilibili player (popular in China). The `{{video}}` macro auto-detects Bilibili URLs.

---

## Advanced Usage

### Multiple Videos in One Block

```markdown
- Conference Talks
  - Keynote: {{video https://youtu.be/abc123}}
  - Workshop: {{video https://youtu.be/def456}}
  - Q&A: {{video https://youtu.be/ghi789}}
```

**Result**: Three separate embedded players stacked vertically

### Video with Timestamps as Notes

```markdown
- [[Project Demo Video]]
  - {{video https://www.youtube.com/watch?v=dQw4w9WgXcQ}}
  - Timestamps:
    - {{youtube-timestamp 0:00}} — Introduction
    - {{youtube-timestamp 1:30}} — Feature overview
    - {{youtube-timestamp 5:45}} — Demo walkthrough
    - {{youtube-timestamp 10:00}} — Q&A
```

**Workflow**: Click timestamp to jump to that section of the video.

### Transcription Workflow

```markdown
- [[Meeting Recording]]
  - {{video ../assets/meeting-2024-01-20.mp4}}
  - Transcript:
    - {{youtube-timestamp 0:00}} **Alice**: Let's start with project updates
    - {{youtube-timestamp 2:15}} **Bob**: We completed the frontend redesign
    - {{youtube-timestamp 5:30}} **Charlie**: Database migration is 80% done
```

### Lazy Loading (Performance)

**Problem**: Too many videos on one page slow down loading.

**Solution**: Use block references instead of direct embeds:

```markdown
- [[Video Library]]
  - Introduction Video: ((block-ref-to-video-1))
  - Tutorial Series: ((block-ref-to-video-2))
  - Advanced Topics: ((block-ref-to-video-3))
```

Create video blocks separately and reference them. Videos only load when blocks are expanded.

---

## Platform-Specific Notes

### Desktop (Electron)

**Features**:

- ✅ Full YouTube API support
- ✅ Timestamp controls work perfectly
- ✅ Local video playback with hardware acceleration
- ✅ Fullscreen mode
- ✅ Multiple videos on same page

**Best Experience**: Desktop is the recommended platform for video features.

### Web Browser

**Features**:

- ✅ YouTube embeds work
- ✅ Local videos work (if uploaded to server)
- ⚠️ CORS restrictions may block some external videos
- ⚠️ File protocol (`file://`) doesn't work in web browsers

**Limitation**: Cannot access local file system directly (security restriction).

### Mobile (iOS/Android)

**Features**:

- ✅ YouTube embeds work (using wrapper page)
- Timestamp controls don't work (see [youtube.cljs:139](../../src/main/frontend/extensions/video/youtube.cljs#L139))
- ⚠️ Local video playback may have codec issues
- ⚠️ Fullscreen may exit Logseq app (platform behavior)

**Known Issues**:

- YouTube timestamp links show error: "YouTube timestamps aren't available on mobile yet"
- Some video formats don't play (device-dependent)

**Code Reference**: [`youtube.cljs:29-30`](../../src/main/frontend/extensions/video/youtube.cljs#L29-L30)

```clojure
(defn- use-youtube-wrapper? []
  (mobile-util/native-platform?))
```

---

## Troubleshooting

### Video Won't Load

**Problem**: YouTube video shows "Video unavailable"

**Possible Causes**:

1. Video is private or deleted
2. Geographic restrictions (VPN may help)
3. YouTube embed disabled by uploader
4. Ad blocker interfering

**Solutions**:

- Test video URL in browser first
- Try different video
- Check YouTube video settings (creator controls)
- Disable browser extensions temporarily

### Local Video Won't Play

**Problem**: Video file embedded but doesn't play

**Possible Causes**:

1. Unsupported codec (e.g., HEVC/H.265)
2. File path incorrect
3. File corrupted
4. Browser codec limitations

**Solutions**:

- Convert to MP4 with H.264 codec: `ffmpeg -i input.mov -c:v libx264 -c:a aac output.mp4`
- Check file path: `../assets/video.mp4` (relative) or absolute path
- Try opening video in browser directly
- Check browser console for errors (Help → Toggle Developer Tools)

### Timestamp Links Don't Work

**Problem**: Clicking {{youtube-timestamp}} does nothing

**Platform Check**:

- **Mobile**: This is a known limitation (see Platform-Specific Notes)
- **Desktop**: Player may not be ready yet

**Solutions**:

- Wait for video to fully load
- Reload page
- Ensure video is from YouTube (timestamps only work with YouTube)
- Check that player is visible on page

### Video Too Large

**Problem**: Video file size exceeds graph limits

**Solutions**:

1. **Compress video**:
   ```bash
   ffmpeg -i input.mp4 -vcodec h264 -acodec aac -crf 28 output.mp4
   ```
2. **Upload to YouTube** and embed instead:

   - Upload unlisted video to YouTube
   - Get share link
   - Use `{{video URL}}` syntax

3. **Use external hosting**:
   - Upload to Vimeo, Dropbox, Google Drive
   - Embed public link

**Size Limits**:

- Assets: 100 MB per file (see [assets.cljs:22-25](../../src/main/frontend/handler/assets.cljs#L22-L25))
- RTC sync: Large assets slow down collaboration

---

## Best Practices

### Performance

✅ **Do**:

- Use YouTube/Vimeo for long videos (> 5 minutes)
- Keep local videos under 50 MB for quick loading
- Use lazy loading (block references) for video libraries
- Convert videos to MP4 H.264 for best compatibility

❌ **Don't**:

- Embed 10+ videos on one page (causes lag)
- Upload 4K videos directly (compress first)
- Use FLV, AVI, or MKV formats (limited browser support)

### Organization

**Create Video Library**:

```markdown
- [[📹 Video Library]]
  - [[Tutorials]]
    - Beginner: {{video URL1}}
    - Intermediate: {{video URL2}}
    - Advanced: {{video URL3}}
  - [[Conference Talks]]
  - [[Project Demos]]
```

**Use Tags**:

```markdown
- {{video URL}} #tutorial #beginner #5min
```

**Timestamp Index**:

```markdown
- [[Course: Introduction to Logseq]]
  - Lesson 1: {{video URL1}}
    - {{youtube-timestamp 0:00}} Overview
    - {{youtube-timestamp 5:30}} Installation
    - {{youtube-timestamp 10:00}} First graph
```

### Documentation

**Video + Text Pattern**:

```markdown
- [[Feature Documentation]]
  - **Watch Demo**: {{video DEMO_URL}}
  - **Steps**:
    - 1. Click Settings
    - 2. Navigate to Sync
    - 3. Enable feature
  - **See also**: [[Written Tutorial]]
```

**Presentation Archive**:

```markdown
- [[Team Presentations]]
  - 2024-01-20: Q4 Roadmap
    - Recording: {{video ../assets/roadmap-q4.mp4}}
    - Slides: ![](../assets/roadmap-slides.pdf)
    - Notes: [[Meeting Notes 2024-01-20]]
```

---

## Code Reference

**Core Implementation**: [`src/main/frontend/extensions/video/youtube.cljs`](../../src/main/frontend/extensions/video/youtube.cljs) (212 lines)

**Key Functions**:

- `youtube-video` (line 53-92) — Main YouTube embed component
- `timestamp` (line 139-154) — Clickable timestamp links
- `load-youtube-api` (line 20-27) — YouTube IFrame API loader
- `get-player` (line 113-127) — Get player instance for controls
- `seconds->display` (line 94-105) — Format seconds as HH:MM:SS

**Configuration**: [`src/main/frontend/config.cljs`](../../src/main/frontend/config.cljs)

- `video-formats` — Supported video file extensions

**Asset Handling**: [`src/main/frontend/handler/assets.cljs`](../../src/main/frontend/handler/assets.cljs)

- `exceed-limit-size?` (line 22-25) — 100 MB asset limit check
- `normalize-asset-resource-url` — Convert paths to URLs

---

## Related Documentation

**Tutorials**:

- [Basic Editing](../../tutorials/getting-started/basic-editing.md) — Learn markdown syntax including media embeds

**How-To Guides**:

- [Asset Management](../multimedia/asset-management.md) — Organizing and managing video files
- [Publishing Workflow](../integrations/publishing.md) — Publishing graphs with embedded videos

**References**:

- [Markdown Extensions](../../reference/syntax/markdown-extensions.md) — All macro syntax including `{{video}}`
- [Asset Class](../../reference/schema/built-in-classes.md#logseqclassasset) — Asset storage in database graphs

**Explanations**:

- [Handbooks System](../../explanation/features/handbooks-system.md) — Interactive tutorials with videos

---

**Last Updated**: March 2026  
**Status**: 🟢 Stable (Video embedding mature feature)  
**Feedback**: Report video issues on [GitHub](https://github.com/logseq/logseq/issues) with `video` label
