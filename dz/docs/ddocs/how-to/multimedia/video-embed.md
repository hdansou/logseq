---
title: 'Video Embedding in Logseq'
status: '🟢 Stable'
quadrant: 'How-To Guide'
platform: 'Desktop (macOS/Windows/Linux), Web'
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
related-docs:
  - '../multimedia/asset-management.md'
  - '../../reference/formatting/markdown-syntax.md'
  - '../../tutorials/getting-started/03-basic-editing.md'
---

# Video Embedding in Logseq

> **Last revised:** 2026-09-09 (verified against `be800f1711`)

## Overview

Logseq embeds videos from four remote provider families plus local video files. Remote videos
render as an iframe player inside your block; local files render as a native HTML5 `<video>`
element.

**Supported remote providers** — this is the complete list, from `matched-video-provider`
([`src/main/frontend/extensions/video.cljs:19-39`](../../../../../src/main/frontend/extensions/video.cljs)):

| Provider           | Domains matched                                  | Rendered as                    |
| ------------------ | ------------------------------------------------ | ------------------------------ |
| YouTube            | `youtube.com`, `youtu.be`, `y2u.be` (also `www.`/`m.`) | YouTube IFrame API player |
| YouTube-nocookie   | `youtube-nocookie.com`                           | plain iframe                   |
| Bilibili           | `bilibili.com`                                   | plain iframe                   |
| Vimeo              | `vimeo.com`, `player.vimeo.com`                  | plain iframe                   |
| Loom               | `loom.com`                                       | plain iframe                   |

**Local video files**: `.mp4`, `.webm`, `.mov`, `.flv`, `.avi`, `.mkv`
(`config/video-formats`, [`src/main/frontend/config.cljs:183-184`](../../../../../src/main/frontend/config.cljs)).

**Macros available**: `{{video}}`, `{{youtube}}`, `{{vimeo}}`, `{{bilibili}}` and
`{{youtube-timestamp}}`.

> **Deleted (was invented):** there is no `{{loom}}` macro. The macro name set is exactly
> `#{"video" "youtube" "vimeo" "bilibili"}`
> ([`src/main/frontend/components/block/video.cljs:6`](../../../../../src/main/frontend/components/block/video.cljs),
> dispatched at [`components/block.cljs:1879-1904`](../../../../../src/main/frontend/components/block.cljs)).
> Loom is reachable only through `{{video}}` auto-detection.

## Findings: New Capabilities

- **Video embeds are drag-resizable.** Every non-mobile embed gets a resize handle; dragging
  rewrites the macro in the block as `{{video URL, w=NNN}}`, and double-clicking the handle resets
  to the 560px default
  ([`components/block.cljs:1669-1719`](../../../../../src/main/frontend/components/block.cljs),
  persisted via `update-video-macro-width-in-content`,
  [`components/block/video.cljs:80-103`](../../../../../src/main/frontend/components/block/video.cljs)).
- **`w=` is a real macro argument**, not just a by-product of dragging — you can type it
  (`video-width`, [`components/block/video.cljs:40-46`](../../../../../src/main/frontend/components/block/video.cljs)).
- **Pasting any supported video URL auto-wraps it** as `{{video URL}}` — not only YouTube
  ([`src/main/frontend/handler/paste.cljs:84-92`](../../../../../src/main/frontend/handler/paste.cljs)).
- **YouTube Shorts and `m.youtube.com` are matched**, as is `y2u.be`
  ([`extensions/video.cljs:7`](../../../../../src/main/frontend/extensions/video.cljs); covered by
  [`src/test/frontend/extensions/video_test.cljs:5-15`](../../../../../src/test/frontend/extensions/video_test.cljs)).

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

**Result**: Embedded YouTube player, 560px wide with a 16:9 aspect ratio.

There is also a slash command, **"Embed Video URL"**, which inserts `{{video }}` with the cursor
placed inside ([`src/main/frontend/commands.cljs:345-348`](../../../../../src/main/frontend/commands.cljs)).

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

**Result**: Native HTML5 `<video>` element with controls.

> **Corrected:** `{{video ../assets/my-video.mp4}}` does **not** work. `{{video}}` with no provider
> hint requires a `protocol://` URL — `common-util/url?`
> ([`deps/common/src/logseq/common/util.cljs:77-86`](../../../../../deps/common/src/logseq/common/util.cljs)) —
> and anything else renders an "Invalid URL" warning
> ([`components/block.cljs:1832-1844`](../../../../../src/main/frontend/components/block.cljs),
> string at [`src/resources/dicts/en.edn:128`](../../../../../src/resources/dicts/en.edn)).
> Even a well-formed URL only embeds if it matches one of the five providers above. Use the
> `![...](...)` asset syntax for local files.

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

**Shorts URL**:

```markdown
{{video https://www.youtube.com/shorts/dQw4w9WgXcQ}}
```

**Bare video ID** (only with the `{{youtube}}` macro, which supplies the provider hint):

```markdown
{{youtube dQw4w9WgXcQ}}
```

A bare ID is accepted for `{{youtube}}` only when it is exactly 11 characters, for `{{bilibili}}`
when it is 15 characters or fewer, and for `{{vimeo}}` when it is all digits
([`extensions/video.cljs:53-65`](../../../../../src/main/frontend/extensions/video.cljs)).

### YouTube with Timestamps

**Start at a specific time**:

```markdown
{{video https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=90}}
```

**How `t=` is parsed**: `video-start` matches `[?&]t=(\d+)` and keeps only the digits
([`extensions/video.cljs:49-51`](../../../../../src/main/frontend/extensions/video.cljs)).
Consequences:

- `t=90` → starts at 90 seconds.
- `t=90s` → also starts at 90 seconds; the trailing `s` is simply not captured.
- `t=1m30s` → starts at **1 second**, not 90. The regex captures the first digit run and stops.
  Convert to total seconds yourself.

**Where `t=` applies**: YouTube (as the player's `start` parameter), YouTube-nocookie (`?t=`) and
Bilibili (`&t=`). **Vimeo and Loom ignore it** — their embed URLs are built without any start
parameter ([`extensions/video.cljs:74-103`](../../../../../src/main/frontend/extensions/video.cljs)).

### Custom Player Size

**Corrected:** size *is* directly supported in macro syntax. Pass a `w=` argument, comma-separated
from the URL:

```markdown
{{video https://www.youtube.com/watch?v=dQw4w9WgXcQ, w=800}}
```

Arguments are split on commas
([`components/block/video.cljs:68-72`](../../../../../src/main/frontend/components/block/video.cljs))
and `w=` must be `w=<digits>`
([`:40-46`](../../../../../src/main/frontend/components/block/video.cljs)).

You can also just **drag the right edge of the embed**. The handle writes the resulting `w=` back
into the block text, and double-clicking it resets to the default
([`components/block.cljs:1669-1719, 1802-1826`](../../../../../src/main/frontend/components/block.cljs)).
The handle is not rendered on native mobile
([`:1726`](../../../../../src/main/frontend/components/block.cljs)).

**Sizes**:

- Default width: **560px** (`default-video-embed-width`,
  [`components/block.cljs:1651`](../../../../../src/main/frontend/components/block.cljs))
- Minimum width: **160px** (`min-video-embed-width`, [`:1652`](../../../../../src/main/frontend/components/block.cljs))
- Maximum width: the embed's parent element width, falling back to `(- (window width) 96)`
  ([`:1727-1728`](../../../../../src/main/frontend/components/block.cljs))
- Height: derived from `aspect-ratio: 16 / 9` in CSS, not set directly
  ([`:1732-1734`](../../../../../src/main/frontend/components/block.cljs))

> **Deleted (stale):** the old claim that width "adapts to the container (max 560px)" via
> `(min (- (util/get-width) 96) 560)` in `youtube.cljs`. That expression is no longer in
> `youtube.cljs` at all; it now lives in `asset-video-style`
> ([`components/block.cljs:475`](../../../../../src/main/frontend/components/block.cljs)) and applies
> to **local asset videos**, not YouTube embeds. The YouTube component itself now takes a flat
> `default-video-width` of 560 and `default-video-height` of 315
> ([`extensions/video/youtube.cljs:53-59`](../../../../../src/main/frontend/extensions/video/youtube.cljs)).

The iframe still carries a stable id, so CSS targeting works:

```css
/* In custom.css — target by iframe id */
#youtube-player-dQw4w9WgXcQ {
  width: 800px;
}
```

([`extensions/video/youtube.cljs:85`](../../../../../src/main/frontend/extensions/video/youtube.cljs).
Prefer `w=` or the drag handle; the surrounding `.video-embed-frame` sets the real box.)

### YouTube Timestamp Links

**Creating clickable timestamps**:

```markdown
- Watch intro at {{youtube-timestamp 0:30}}
- Jump to main section at {{youtube-timestamp 2:45}}
- See conclusion at {{youtube-timestamp 1:10:15}}
- Or just give it seconds: {{youtube-timestamp 150}}
```

`parse-timestamp` accepts a bare integer (total seconds), `mm:ss`, or `hh:mm:ss`
([`extensions/video/youtube.cljs:185-201`](../../../../../src/main/frontend/extensions/video/youtube.cljs)).
Anything else parses to `nil` and the macro renders nothing.

**Behavior**:

- Clicking a timestamp seeks the video to that point.
- `get-player` walks the page's iframes, keeps those whose `src` points at a YouTube embed, and
  takes the **last one that appears before the clicked timestamp** in document order
  ([`youtube.cljs:120-134`](../../../../../src/main/frontend/extensions/video/youtube.cljs)) — so a
  timestamp controls the nearest preceding YouTube embed, not "the most recently played" one.
- Displays as `mm:ss` or `hh:mm:ss` (`seconds->display`, [`:101-112`](../../../../../src/main/frontend/extensions/video/youtube.cljs)).

**Inserting one at the current playhead**: the slash command **"Embed YouTube timestamp"**
([`commands.cljs:349`](../../../../../src/main/frontend/commands.cljs)) or the shortcut
`mod+shift+y` ([`src/main/frontend/modules/shortcut/config.cljs:164-165`](../../../../../src/main/frontend/modules/shortcut/config.cljs)).

**Mobile limitation**: timestamp controls do not work on the native iOS/Android apps. Clicking one
shows *"YouTube timestamps aren't available on mobile yet."*
([`src/resources/dicts/en.edn:2003`](../../../../../src/resources/dicts/en.edn)).

**Code Reference**: [`youtube.cljs:146-161`](../../../../../src/main/frontend/extensions/video/youtube.cljs#L146-L161)

```clojure
(hsx/defc timestamp
  [seconds]
  [:a.youtube-timestamp
   {:on-click (fn [e]
                (util/stop e)
                (if (use-youtube-wrapper?)
                  (notify-timestamp-unavailable!)
                  (when-let [player (get-player (.-target e))]
                    (if-let [seek-to (player-method player "seekTo")]
                      (.call seek-to player seconds true)
                      (notification/show!
                       (t :youtube/player-not-ready)
                       :warning
                       false)))))}
   [:span.youtube-timestamp-icon svg/clock]
   [:span.youtube-timestamp-label (seconds->display seconds)]])
```

### YouTube API Integration

**Desktop/Web only**: Logseq loads the YouTube IFrame API and registers a player per embed, which
is what makes seeking possible.

**Features enabled**:

- Programmatic playback control (`seekTo`, `getCurrentTime`)
- Timestamp seeking
- `onReady` event handling — players are stored in app state under `[:youtube/players]`
- Multiple players on the same page, keyed by video id

**Code Reference**: [`youtube.cljs:22-29`](../../../../../src/main/frontend/extensions/video/youtube.cljs#L22-L29)

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

**Mobile limitation**: native iOS/Android apps load
`https://logseq.com/youtube.html?v=<id>&enablejsapi=1` instead of the direct embed, and skip API
registration entirely ([`youtube.cljs:31-32, 71-75`](../../../../../src/main/frontend/extensions/video/youtube.cljs)).

---

## Local Video Files

### Supported Formats

```clojure
(def video-formats
  #{:mp4 :webm :mov :flv :avi :mkv})
```

([`src/main/frontend/config.cljs:183-184`](../../../../../src/main/frontend/config.cljs))

Logseq passes the file straight to an HTML5 `<video>` element, so actual playability is the
browser's decision, not Logseq's:

| Format          | Extension | Notes                                                    |
| --------------- | --------- | -------------------------------------------------------- |
| MP4 (H.264)     | `.mp4`    | **Recommended** — best compatibility                      |
| WebM (VP8/VP9)  | `.webm`   | Good compression, open format                             |
| MOV (QuickTime) | `.mov`    | Plays where the browser supports the contained codecs     |
| FLV             | `.flv`    | Recognised by Logseq; browsers generally cannot play it   |
| AVI             | `.avi`    | Recognised by Logseq; browser support varies              |
| MKV             | `.mkv`    | Recognised by Logseq; browser support is limited          |

> **Note:** a `.webm` file whose title starts with `Audio-` is rendered as an **audio** element
> instead ([`components/block.cljs:582-584`](../../../../../src/main/frontend/components/block.cljs)).

**Recommendation**: use MP4 with the H.264 codec for maximum compatibility.

### Adding Local Videos

**Step 1: get the file into the graph's assets**

In a DB graph, drag and drop the file into a block, or use the app's upload affordance. Assets live
under the graph directory's `assets/` folder.

**Step 2: embed it**

```markdown
![Demo Video](../assets/my-video.mp4)
```

**Result**: `<video class="asset-video" controls>` with the browser's native controls
([`components/block.cljs:508-511`](../../../../../src/main/frontend/components/block.cljs)).

### Video Player Controls

The player is a plain HTML5 `<video controls>` element, so play/pause, volume, seeking, elapsed
time, fullscreen and playback-rate controls are whatever the host browser provides. The same goes
for keyboard shortcuts — Logseq registers none of its own for the video element.

> **Unverified:** the specific keyboard shortcut list previously given here (`Space`, arrow keys,
> `F`, `M`) was not sourced from Logseq code, because Logseq does not implement it. Behaviour is
> browser- and platform-dependent.

### Video Sizing

```markdown
![](../assets/video.mp4){:width 800}
![](../assets/video.mp4){:height 450}
![](../assets/video.mp4){:width 800 :height 450}
```

The metadata map feeds `asset-video-style`
([`components/block.cljs:471-485`](../../../../../src/main/frontend/components/block.cljs)), which:

- clamps width to `(min (- (window width) 96) 560)` — so `{:width 800}` renders at **at most
  560px**, not 800;
- clamps height to 557px, preserving the aspect ratio;
- falls back to `{:max-width …}` when width and height are not both known.

Logseq also measures the video once and stores its true dimensions on the asset block as
`:logseq.property.asset/width` / `:logseq.property.asset/height`
([`:493-507`](../../../../../src/main/frontend/components/block.cljs)); a saved
`:logseq.property.asset/resize-metadata` overrides the inline metadata
([`src/main/frontend/components/block/image.cljs:4-11`](../../../../../src/main/frontend/components/block/image.cljs)).

---

## Other Platforms

### Vimeo

```markdown
{{video https://vimeo.com/123456789}}
{{vimeo https://vimeo.com/123456789}}
{{vimeo 123456789}}
```

**Result**: iframe pointing at `https://player.vimeo.com/video/<id>`
([`extensions/video.cljs:95-98`](../../../../../src/main/frontend/extensions/video.cljs)).
No start-time parameter is added, so `t=` has no effect.

### Loom

```markdown
{{video https://www.loom.com/share/VIDEO_ID}}
{{video https://www.loom.com/embed/VIDEO_ID}}
```

**Result**: iframe pointing at `https://www.loom.com/embed/<id>`
([`extensions/video.cljs:100-103`](../../../../../src/main/frontend/extensions/video.cljs)).
Only `/share/` and `/embed/` paths match
([`:5`](../../../../../src/main/frontend/extensions/video.cljs)). There is no `{{loom}}` macro.

### Bilibili

```markdown
{{video https://www.bilibili.com/video/BV1JN4y127nk}}
{{bilibili BV1JN4y127nk}}
{{video https://www.bilibili.com/video/BV1JN4y127nk?p=2&t=10}}
```

**Result**: iframe pointing at
`https://player.bilibili.com/player.html?bvid=<id>&high_quality=1&autoplay=0`, with `&p=` appended
for a multi-part video and `&t=` for a start time
([`extensions/video.cljs:88-93`](../../../../../src/main/frontend/extensions/video.cljs); asserted in
[`video_test.cljs:34-46`](../../../../../src/test/frontend/extensions/video_test.cljs)).

### YouTube-nocookie

```markdown
{{video https://www.youtube-nocookie.com/embed/dQw4w9WgXcQ}}
```

**Result**: a plain iframe against `youtube-nocookie.com/embed/<id>`. Because it is a plain iframe
rather than an API-registered player, `{{youtube-timestamp}}` seeking still finds it by `src`
([`youtube.cljs:127`](../../../../../src/main/frontend/extensions/video/youtube.cljs)) but no
player object is registered for it.

---

## Advanced Usage

### Multiple Videos in One Block

```markdown
- Conference Talks
  - Keynote: {{video https://youtu.be/abc12345678}}
  - Workshop: {{video https://youtu.be/def45678901}}
  - Q&A: {{video https://youtu.be/ghi78901234}}
```

Each video macro is lifted into its own `.video-embed-block` wrapper, with any surrounding inline
text kept in sibling `.video-inline-text` elements
([`components/block/video.cljs:105-147`](../../../../../src/main/frontend/components/block/video.cljs)).
That wrapper carries `data-video-macro-name` and `data-video-macro-id` attributes, which is how a
resize knows *which* of several identical macros in one block to rewrite
([`components/block.cljs:1769-1784`](../../../../../src/main/frontend/components/block.cljs)).

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

The timestamps must appear **after** the embed in document order, since `get-player` only considers
iframes that precede the clicked element.

### Transcription Workflow

```markdown
- [[Meeting Recording]]
  - {{video https://www.youtube.com/watch?v=dQw4w9WgXcQ}}
  - Transcript:
    - {{youtube-timestamp 0:00}} **Alice**: Let's start with project updates
    - {{youtube-timestamp 2:15}} **Bob**: We completed the frontend redesign
```

> **Corrected:** the previous version of this example paired `{{youtube-timestamp}}` with a **local
> asset video**. That cannot work — `get-player` only ever returns YouTube iframe players
> ([`youtube.cljs:120-134`](../../../../../src/main/frontend/extensions/video/youtube.cljs)), so
> clicking a timestamp next to an `.mp4` does nothing.

---

## Platform-Specific Notes

### Desktop (Electron)

- ✅ Full YouTube IFrame API support
- ✅ Timestamp seeking
- ✅ Drag-to-resize embeds
- ✅ Local video playback
- ✅ Multiple players on the same page

### Web Browser

- ✅ YouTube embeds work
- ✅ Drag-to-resize embeds (the handle is suppressed only on **native** mobile, not in a mobile browser)
- ⚠️ Local assets must be reachable by the browser; `file://` is not
- ⚠️ Ad blockers and privacy extensions can block the YouTube IFrame API script

### Mobile (iOS/Android native apps)

- ✅ YouTube embeds work, via the `logseq.com/youtube.html` wrapper page
- ❌ Timestamp controls do not work
- ❌ No drag-to-resize handle
- ⚠️ Local video playback depends on device codec support

**Code Reference**: [`youtube.cljs:31-32`](../../../../../src/main/frontend/extensions/video/youtube.cljs#L31-L32)

```clojure
(defn- use-youtube-wrapper? []
  (mobile-util/native-platform?))
```

Related messages ([`src/resources/dicts/en.edn:2001-2003`](../../../../../src/resources/dicts/en.edn)):

- `:youtube/timestamps-not-available-mobile` — "YouTube timestamps aren't available on mobile yet."
- `:youtube/player-not-ready` — "YouTube player isn't ready yet."
- `:youtube/embed-first-reminder-mobile` — shown when you try to insert a timestamp with no embed present.

---

## Troubleshooting

### The macro renders as a warning instead of a player

| Warning       | Cause                                                                    |
| ------------- | ------------------------------------------------------------------------ |
| "Empty URL"   | `{{video}}` with no argument ([`en.edn:122`](../../../../../src/resources/dicts/en.edn)) |
| "Invalid URL" | The argument is not a `protocol://` URL and no provider hint was supplied ([`en.edn:128`](../../../../../src/resources/dicts/en.edn)) |
| Nothing at all | The URL is valid but matches none of the five providers — `matched-video-embed` returns `nil` ([`extensions/video.cljs:67-105`](../../../../../src/main/frontend/extensions/video.cljs)) |

### Video Won't Load

**Problem**: YouTube video shows "Video unavailable"

**Possible causes**: the video is private or deleted; geographic restrictions; the uploader
disabled embedding; an ad blocker is interfering with `youtube.com/iframe_api`.

**Solutions**: test the URL in a browser first; check the creator's embed settings; temporarily
disable browser extensions.

### Local Video Won't Play

**Possible causes**: unsupported codec (e.g. HEVC/H.265); wrong path; corrupt file; browser codec
limitations — Logseq recognising an extension is not the same as the browser decoding it.

**Solutions**:

- Convert to MP4/H.264: `ffmpeg -i input.mov -c:v libx264 -c:a aac output.mp4`
- Check the path resolves relative to the block's page
- Open the file directly in a browser to isolate the problem
- Check the developer console (Help → Toggle Developer Tools)

### Timestamp Links Don't Work

- **Native mobile**: expected — see Platform-Specific Notes.
- **Desktop/web**: the player may not be registered yet; you will see "YouTube player isn't ready yet."
- The timestamp must sit **after** a YouTube embed in the same page.
- Timestamps only drive YouTube players. Vimeo, Loom, Bilibili and local files are unaffected.

### Video Too Large

1. **Compress**:
   ```bash
   ffmpeg -i input.mp4 -vcodec h264 -acodec aac -crf 28 output.mp4
   ```
2. **Host it remotely** on one of the supported providers and embed the link instead.

> **Kept from previous review — still true:** direct embedding from Dropbox or Google Drive share
> links is not supported. Those domains match none of the four provider regexes
> ([`extensions/video.cljs:4-7`](../../../../../src/main/frontend/extensions/video.cljs)).

**Size limit**: assets are capped at **100 MB per file**
([`exceed-limit-size?`, `src/main/frontend/handler/assets.cljs:14-17`](../../../../../src/main/frontend/handler/assets.cljs)).

---

## Best Practices

✅ **Do**:

- Prefer a supported provider for long videos rather than committing a large file to the graph
- Convert local videos to MP4/H.264
- Use `w=` (or the drag handle) rather than fighting the default with CSS
- Put `{{youtube-timestamp}}` blocks below their embed

❌ **Don't**:

- Expect `{{video}}` to play a local file — use `![](../assets/…)`
- Expect `{:width 800}` on a local video to exceed 560px
- Expect `t=1m30s` to mean 90 seconds
- Rely on FLV, AVI or MKV playing in the browser

### Organization

**Timestamp index**:

```markdown
- [[Course: Introduction to Logseq]]
  - Lesson 1: {{video https://youtu.be/dQw4w9WgXcQ, w=720}}
    - {{youtube-timestamp 0:00}} Overview
    - {{youtube-timestamp 5:30}} Installation
    - {{youtube-timestamp 10:00}} First graph
```

---

## Code Reference

**URL matching and embed construction**:
[`src/main/frontend/extensions/video.cljs`](../../../../../src/main/frontend/extensions/video.cljs) (105 lines)

- `get-matched-video` (4-17) — the four provider regexes, tried in order
- `matched-video-provider` (19-39) — maps a matched domain to one of five provider keywords
- `video-start` (49-51) — extracts `t=<digits>`
- `matched-video-embed` (67-105) — returns `{:render … :src/:id … :aspect-ratio [16 9]}`

**YouTube player**:
[`src/main/frontend/extensions/video/youtube.cljs`](../../../../../src/main/frontend/extensions/video/youtube.cljs) (217 lines)

- `load-youtube-api` (22-29) — IFrame API loader
- `register-player` (34-51) — stores the player under `[:youtube/players]`
- `youtube-video` (56-99) — the embed component
- `seconds->display` (101-112) — formats seconds as `mm:ss` / `hh:mm:ss`
- `get-player` (120-134) — finds the nearest preceding YouTube iframe
- `timestamp` (146-161) — clickable timestamp links
- `gen-youtube-ts-macro` (163-183) — builds `{{youtube-timestamp N}}` at the current playhead
- `parse-timestamp` (185-201) — accepts seconds, `mm:ss`, `hh:mm:ss`

**Macro layout and width persistence**:
[`src/main/frontend/components/block/video.cljs`](../../../../../src/main/frontend/components/block/video.cljs) (147 lines)

**Macro dispatch, embed shell and resize**:
[`src/main/frontend/components/block.cljs:1651-1847, 1879-1904`](../../../../../src/main/frontend/components/block.cljs)

**Configuration**: `video-formats` in
[`src/main/frontend/config.cljs:183-184`](../../../../../src/main/frontend/config.cljs)

**Asset handling**: `exceed-limit-size?` in
[`src/main/frontend/handler/assets.cljs:14-17`](../../../../../src/main/frontend/handler/assets.cljs)

**Tests**:
[`src/test/frontend/extensions/video_test.cljs`](../../../../../src/test/frontend/extensions/video_test.cljs),
[`src/test/frontend/components/block/video_test.cljs`](../../../../../src/test/frontend/components/block/video_test.cljs)

---

## Related Documentation

**Tutorials**:

- [Basic Editing](../../tutorials/getting-started/03-basic-editing.md) — markdown syntax including media embeds

**How-To Guides**:

- [Asset Management](../multimedia/asset-management.md) — organizing and managing video files

**References**:

- [Markdown Extensions](../../reference/formatting/markdown-syntax.md) — all macro syntax including `{{video}}`

---

**Last Updated**: 2026-09-09 — verified against `be800f1711`
**Status**: 🟢 Stable
**Feedback**: Report video issues on [GitHub](https://github.com/logseq/logseq/issues) with the `video` label
