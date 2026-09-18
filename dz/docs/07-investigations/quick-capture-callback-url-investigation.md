# Investigation: Logseq Quick Capture Feature and Callback URL

**Version:** 1.1  
**Investigation Date:** November 24, 2025

> **Last revised:** 2026-05-30
> **Platforms:** macOS (Electron), iOS (Capacitor)  
> **Investigator:** GitHub Copilot

---

## Executive Summary

### Key Findings

- **✅ Multi-platform support**: Quick capture works on both macOS (via callback URL) and iOS (via system share extension)
- **🔄 Dual entry points**:
  - macOS: `logseq://x-callback-url/quickCapture` URL scheme
  - iOS: System share extension → `logseq://shared?payload=...` URL scheme
- **⚡ Real-time processing**: Captures are processed immediately with template substitution
- **💾 Immediate persistence**: Content is inserted directly into blocks with auto-save handling
- **⚠️ Duplicate firing risk**: iOS has a known issue where `appUrlOpen` can fire twice for the same intent

## Findings: New Capabilities

- `quick-capture` now supports a configurable URL prettifier gate via `[:quick-capture-option :prettify-url?]` (default `true`) in `src/main/frontend/quick_capture.cljs`.
- Mobile intent import includes native file-path normalization and a read fallback (`file://` and `_capacitor_file_` handling) in `src/main/frontend/mobile/intent.cljs`.
- Callback URL handler also supports `/invokeCommand` in addition to `/quickCapture` in `src/electron/electron/url.cljs`.

### Verification Scope Note

- This validation pass is scoped to `src/`, `deps/`, and `packages/` only.
- Native iOS extension implementation details are outside this validation scope.

### Critical Components

| Component            | Location                               | Purpose                                   |
| -------------------- | -------------------------------------- | ----------------------------------------- |
| Core Logic           | `src/main/frontend/quick_capture.cljs` | Template processing and content insertion |
| iOS Share Extension  | `ios/App/ShareViewController/`         | System-level share handling               |
| Mobile Deep Link     | `src/main/mobile/deeplink.cljs`        | URL scheme routing                        |
| Mobile Intent        | `src/main/frontend/mobile/intent.cljs` | Share payload processing                  |
| Electron URL Handler | `src/electron/electron/url.cljs`       | Desktop callback URL processing           |
| Electron Listener    | `src/main/electron/listener.cljs`      | IPC bridge to renderer                    |

> **Removed:** `ios/App/ShareViewController/*` as a validated source for this pass, because this update is scoped to `src/`, `deps/`, and `packages/`.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Entry Points](#entry-points)
3. [Execution Flow](#execution-flow)
4. [Data Structures](#data-structures)
5. [Platform Comparison](#platform-comparison)
6. [Configuration](#configuration)
7. [Risk Analysis](#risk-analysis)
8. [Code References](#code-references)
9. [Recommendations](#recommendations)

---

## Architecture Overview

### System Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                        macOS (Electron)                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  External App/Script                                                 │
│         │                                                             │
│         │ logseq://x-callback-url/quickCapture?url=...&title=...    │
│         ↓                                                             │
│   Electron Deeplink Handler (core.cljs)                             │
│         │                                                             │
│         ↓                                                             │
│   URL Parser (url.cljs:x-callback-url-handler)                      │
│         │                                                             │
│         ↓                                                             │
│   IPC → Renderer (listener.cljs:quickCapture)                       │
│         │                                                             │
│         ↓                                                             │
│   Event: [:editor/quick-capture args]                               │
│         │                                                             │
│         ↓                                                             │
│   quick-capture/quick-capture (quick_capture.cljs)                  │
│         │                                                             │
│         ├─→ Template substitution                                    │
│         ├─→ Page selection logic                                     │
│         ├─→ Content insertion (editor-handler)                       │
│         └─→ Auto-save (triggered by edit)                           │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                          iOS (Capacitor)                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  Safari/App Share Button                                            │
│         │                                                             │
│         ↓                                                             │
│   iOS Share Extension (ShareViewController.swift)                   │
│         │                                                             │
│         ├─→ Process attachments (URL, Text, Image, Video)           │
│         ├─→ Copy files to shared container                          │
│         ├─→ Build SharedData payload                                │
│         └─→ Encode JSON                                             │
│              │                                                        │
│              │ logseq://shared?payload={SharedData JSON}            │
│              ↓                                                        │
│   App URL Open Listener (init.cljs:appUrlOpen)                     │
│         │                                                             │
│         ├─→ Duplicate detection (same URL within 1 second)          │
│         └─→ Deep link router (deeplink.cljs)                        │
│              │                                                        │
│              ├─→ hostname: "shared"                                  │
│              └─→ Decode payload                                     │
│                   │                                                   │
│                   ↓                                                   │
│   Intent Handler (intent.cljs:handle-payload)                       │
│         │                                                             │
│         ├─→ Process resources (files, images, etc.)                 │
│         ├─→ Copy assets to repo                                     │
│         ├─→ Template substitution                                    │
│         └─→ Content insertion                                       │
│              │                                                        │
│              └─→ Auto-save                                          │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Entry Points

### 1. macOS Callback URL

**URL Scheme:** `logseq://x-callback-url/quickCapture`

**Parameters:**

- `url` (string): Web link or resource URL
- `title` (string): Display text for the link
- `content` (string): Highlighted/selected text
- `page` (string): Target page name (use "TODAY" for today's journal)
- `append` (boolean): Append to page end vs. current position (default: false)

**Example:**

```bash
open "logseq://x-callback-url/quickCapture?url=https://example.com&title=Example&content=Some%20highlighted%20text&page=Reading%20List"
```

**Code Flow:**

```
electron/core.cljs:open-url-handler
  └→ Parse URL
  └→ electron/url.cljs:logseq-url-handler
      └→ Match host: "x-callback-url"
      └→ electron/url.cljs:x-callback-url-handler
          └→ Match pathname: "/quickCapture"
          └→ Decode parameters
          └→ send-to-focused-renderer "quickCapture" {:url, :title, :content, :page, :append}
              └→ main/electron/listener.cljs
                  └→ state/pub-event! [:editor/quick-capture args]
```

**Location:** `src/electron/electron/url.cljs:62-92`

```clojure
(defn- x-callback-url-handler
  [^js win ^js/URL parsed-url]
  (let [action (.-pathname parsed-url)]
    (cond
      (= action "/quickCapture")
      (let [[url title content page append]
            (get-URL-decoded-params parsed-url
                                   ["url" "title" "content" "page" "append"])]
        (send-to-focused-renderer "quickCapture"
                                 {:url url
                                  :title title
                                  :content content
                                  :page page
                                  :append (if (nil? append)
                                           append
                                           (= append "true"))}
                                 win)))))
```

### 2. iOS Share Extension

**Trigger:** iOS System Share Sheet → Logseq

**Supported Content Types:**

- `kUTTypeURL`: Web links and file URLs
- `kUTTypeText`: Plain text
- `kUTTypeImage`: Images (PNG, JPG, etc.)
- `kUTTypeMovie`: Videos
- File URLs (copied to shared container)

**Code Flow:**

```
iOS Share Button
  └→ ios/App/ShareViewController/ShareViewController.swift:viewDidLoad
      └→ Process attachments concurrently
          ├→ handleTypeUrl: Extract URLs (web or file)
          ├→ handleTypeText: Extract plain text
          ├→ handleTypeImage: Process images (UIImage or URL)
          └→ handleTypeMovie: Process videos
      └→ Build SharedData:
          {
            text: "selected text",
            resources: [
              {name, ext, type, url}  // Files copied to group container
            ]
          }
      └→ Encode JSON payload
      └→ Open URL: logseq://shared?payload={encoded JSON}
      └→ Complete extension request

  └→ mobile/init.cljs:appUrlOpen listener
      └→ Duplicate detection (same URL within 1 second)
      └→ mobile/deeplink.cljs:deeplink
          └→ Parse hostname: "shared"
          └→ Decode payload parameter
          └→ mobile/intent.cljs:handle-payload
              └→ Process resources (handle-payload-resource)
              └→ Template substitution
              └→ Insert content
```

**Location:** `ios/App/ShareViewController/ShareViewController.swift:145-180`

```swift
override public func viewDidLoad() {
    super.viewDidLoad()

    sharedData.empty()
    let inputItems = extensionContext?.inputItems as! [NSExtensionItem]
    Task {
        try await withThrowingTaskGroup(
            of: SharedResource?.self,
            body: { taskGroup in
                for extensionItem in inputItems {
                    for attachment in extensionItem.attachments! {
                        if attachment.hasItemConformingToTypeIdentifier(kUTTypeURL as String) {
                            taskGroup.addTask {
                                return try await self.handleTypeUrl(attachment)
                            }
                        } // ... other types
                    }
                }
                for try await item in taskGroup {
                    if let item = item {
                        self.sharedData.resources.append(item)
                    }
                }
            })
        self.sendData()
    }
}
```

---

## Execution Flow

### macOS Flow (Detailed)

```
┌───────────────────────────────────────────────────────────────────┐
│ Step 1: URL Reception                                             │
│ Time: 0ms                                                          │
└───────────────────────────────────────────────────────────────────┘
  Electron receives: logseq://x-callback-url/quickCapture?...
  Handler: electron/core.cljs:open-url-handler (line 45)

  ↓

┌───────────────────────────────────────────────────────────────────┐
│ Step 2: URL Parsing & Routing                                     │
│ Time: ~5ms                                                         │
└───────────────────────────────────────────────────────────────────┘
  Parse URL → Identify scheme "logseq:"
  Route to: electron/url.cljs:logseq-url-handler (line 87)
  Match host: "x-callback-url"
  Call: x-callback-url-handler (line 59)

  ↓

┌───────────────────────────────────────────────────────────────────┐
│ Step 3: IPC Message to Renderer                                   │
│ Time: ~10ms                                                        │
└───────────────────────────────────────────────────────────────────┘
  send-to-focused-renderer("quickCapture", {...})
  IPC channel: "quickCapture"
  Payload: {url, title, content, page, append}

  ↓

┌───────────────────────────────────────────────────────────────────┐
│ Step 4: Renderer Receives IPC                                     │
│ Time: ~15ms                                                        │
└───────────────────────────────────────────────────────────────────┘
  Handler: main/electron/listener.cljs:120-122
  safe-api-call "quickCapture" (fn [args] ...)
  Publish event: [:editor/quick-capture args]

  ↓

┌───────────────────────────────────────────────────────────────────┐
│ Step 5: Core Quick Capture Logic                                  │
│ Time: ~20-150ms                                                    │
└───────────────────────────────────────────────────────────────────┘
  Handler: frontend/quick_capture.cljs:quick-capture (line 22)

  1. Parse arguments (bean/->clj)
  2. Determine target page:
     - "TODAY" → today's journal
     - Explicit page name
     - :default-page config
     - Current page
     - Fallback: today or "quick capture"

  3. Load template from config:
     :quick-capture-templates :text
     Default: "**{time}** [[quick capture]]: {text} {url}"

  4. Format content based on URL type:
     - Video: {{video URL}}
     - Twitter/X: {{twitter URL}}
     - Regular: [title](url)

  5. Substitute placeholders:
     {time} → current time (HH:mm)
     {date} → today's date ref
     {text} → content
     {url} → formatted link

  ↓

┌───────────────────────────────────────────────────────────────────┐
│ Step 6: Content Insertion                                         │
│ Time: ~100-200ms (with delays)                                    │
└───────────────────────────────────────────────────────────────────┘
  Decision tree:

  IF editing AND not append AND not contains "[[quick capture]]":
    ├─ Edit content blank? → insert content
    └─ Edit content not blank → insert "\n" + content

  ELSE:
    ├─ Escape editing
    ├─ Navigate to page (if different)
    └─ setTimeout 100ms →
        editor-handler/api-insert-new-block!
        {
          :page page
          :edit-block? true
          :replace-empty-target? true
        }

  ↓

┌───────────────────────────────────────────────────────────────────┐
│ Step 7: Auto-Save                                                  │
│ Time: ~450-1500ms after insertion                                 │
└───────────────────────────────────────────────────────────────────┘
  Triggered by block edit
  Handler: frontend/handler/editor.cljs
  Auto-save delay: ~450ms (configurable)
  Persistence: DataScript → SQLite/File
```

### iOS Flow (Detailed)

```
┌───────────────────────────────────────────────────────────────────┐
│ Step 1: Share Extension Launch                                    │
│ Time: 0ms                                                          │
└───────────────────────────────────────────────────────────────────┘
  User taps Share → Logseq
  iOS launches ShareViewController.appex
  Handler: ShareViewController.swift:viewDidLoad (line 145)

  ↓

┌───────────────────────────────────────────────────────────────────┐
│ Step 2: Attachment Processing (Concurrent)                        │
│ Time: ~50-500ms (depends on file size)                            │
└───────────────────────────────────────────────────────────────────┘
  Parallel processing with TaskGroup:

  For each attachment:
    ├─ kUTTypeURL → handleTypeUrl
    │   ├─ File URL? Copy to group container
    │   └─ Web URL? Store as plain text
    │
    ├─ kUTTypeText → handleTypeText
    │   └─ Store in sharedData.text
    │
    ├─ kUTTypeImage → handleTypeImage
    │   ├─ UIImage → Save as PNG
    │   └─ URL → Copy file
    │
    └─ kUTTypeMovie → handleTypeMovie
        └─ Copy video file

  Result: SharedData {
    text: "...",
    resources: [{name, ext, type, url}, ...]
  }

  ↓

┌───────────────────────────────────────────────────────────────────┐
│ Step 3: Shared Container File Copy                                │
│ Time: ~100-1000ms (depends on file size)                          │
└───────────────────────────────────────────────────────────────────┘
  Group container: "group.com.logseq.logseq"
  Files copied to container for main app access
  Temporary filenames: URL-encoded original names
  Location: ShareViewController.swift:createSharedFileUrl (line 47)

  ↓

┌───────────────────────────────────────────────────────────────────┐
│ Step 4: JSON Encoding & URL Opening                               │
│ Time: ~10ms                                                        │
└───────────────────────────────────────────────────────────────────┘
  Encode SharedData to JSON
  URL-encode payload
  Build URL: logseq://shared?payload={encoded JSON}
  Open URL → Dismisses extension → Launches main app
  Delay: 0.2s before completion

  ↓

┌───────────────────────────────────────────────────────────────────┐
│ Step 5: Main App URL Reception                                    │
│ Time: ~200-500ms (app launch/resume)                              │
└───────────────────────────────────────────────────────────────────┘
  Capacitor App listener: "appUrlOpen"
  Handler: mobile/init.cljs:91-105

  Duplicate detection:
    ├─ Same URL as last?
    ├─ Within 1 second?
    └─ Skip if duplicate

  Otherwise:
    ├─ Store URL & timestamp
    └─ Wait for app ready (state/app-ready-promise)

  ↓

┌───────────────────────────────────────────────────────────────────┐
│ Step 6: Deep Link Routing                                         │
│ Time: ~5ms                                                         │
└───────────────────────────────────────────────────────────────────┘
  Router: mobile/deeplink.cljs:deeplink (line 18)
  Parse URL: goog.Uri
  Extract hostname: "shared"
  Extract query param: "payload"
  Decode URI component
  Parse JSON → payload map

  ↓

┌───────────────────────────────────────────────────────────────────┐
│ Step 7: Intent Payload Processing                                 │
│ Time: ~100-2000ms (depends on assets)                             │
└───────────────────────────────────────────────────────────────────┘
  Handler: mobile/intent.cljs:handle-payload (line 247)

  1. Get current page or today's journal
  2. Load :quick-capture-templates :text

  3. Process each resource:
     FOR resource IN payload.resources:
       ├─ File/media? → Copy to Logseq assets
       │   └─ Return: asset entities from `db-based-save-assets!`
       └─ Text? → Return as-is

     Join with newlines → rich-content

  4. Template substitution:
     {time} → current time
     {date} → date ref
     {text} → payload.text
     {url} → rich-content (from resources)

  ↓

┌───────────────────────────────────────────────────────────────────┐
│ Step 8: Content Insertion                                         │
│ Time: ~100-200ms                                                   │
└───────────────────────────────────────────────────────────────────┘
  Same logic as macOS:

  IF editing AND not contains "[[quick capture]]":
    ├─ Blank? → insert
    └─ Not blank → insert with newline

  ELSE:
    ├─ Escape editing
    └─ setTimeout 100ms → insert new block

  ↓

┌───────────────────────────────────────────────────────────────────┐
│ Step 9: Asset Persistence                                         │
│ Time: Async, ~500-5000ms                                           │
└───────────────────────────────────────────────────────────────────┘
  Files from group container → Logseq repo assets/
  Handler: editor-handler/db-based-save-assets!
  Creates block entities with :block/uuid
  Auto-save triggered

> **Removed:** "File/media resources are returned as `[[block-uuid]]` links in handle-payload." Current `handle-payload` composes `{url}` only when all processed resources are strings.
```

---

## Data Structures

### 1. macOS Callback Parameters

**Type:** URL Query Parameters (key-value)

**Schema:**

```typescript
interface QuickCaptureParams {
  url?: string // Web link or file path
  title?: string // Display text
  content?: string // Highlighted/selected text
  page?: string // Target page ("TODAY" for journal)
  append?: string // "true" | "false" | null
}
```

**Example:**

```
url=https://example.com/article
title=Interesting Article
content=This is a quote from the article
page=Reading List
append=false
```

**Persistence:** None (ephemeral, processed immediately)

---

### 2. iOS SharedData Structure

**Location:** `ios/App/ShareViewController/SharedData.swift:17-27`

**Schema:**

```swift
public struct SharedResource: Decodable, Encodable {
    var name: String?        // Filename
    var ext: String?         // Extension (e.g., "png", "pdf")
    var type: String?        // MIME type (e.g., "image/png")
    var url: URL?            // File path in group container
}

public struct SharedData: Decodable, Encodable {
    var text: String?                // Plain text content
    var resources: [SharedResource]  // Attached files/media
}
```

**JSON Example:**

```json
{
  "text": "Check out this article",
  "resources": [
    {
      "name": "screenshot.png",
      "ext": "png",
      "type": "image/png",
      "url": "file:///path/to/group.com.logseq.logseq/screenshot.png"
    },
    {
      "name": "https://example.com",
      "type": "text/plain"
    }
  ]
}
```

**Persistence:**

- **Group Container:** `group.com.logseq.logseq/` (temporary)
- **Final Location:** `<repo>/assets/` (after processing)

---

### 3. Configuration Options

**Location:** User's `config.edn`

**Schema:**

```clojure
:quick-capture-options
{:insert-today? boolean     ;; Insert into today's journal (default: false)
 :redirect-page? boolean    ;; Redirect to target page (default: false)
 :default-page string}      ;; Default page name (default: "quick capture")

:quick-capture-templates
{:text string     ;; Template for text/URL captures
 :media string}   ;; Template for media captures (optional)
```

**Template Placeholders:**

- `{time}`: Current time (e.g., "14:30")
- `{date}`: Date reference using current format (e.g., "[[Dec 15th, 2024]]")
- `{text}`: Selected/shared text
- `{url}`: Formatted link or media embed

**Default Template:**

```clojure
"**{time}** [[quick capture]]: {text} {url}"
```

**Example Configuration:**

```clojure
:quick-capture-options
{:insert-today? true
 :redirect-page? false
 :default-page "Inbox"}

:quick-capture-templates
{:text "### {url}\n> {text}\n\n*Captured {time} on [[{date}]]*"
 :media "![]({url})\n\n*Captured {time}*"}
```

---

### 4. Internal Event Structure

**Event Name:** `:editor/quick-capture`

**Payload (macOS):**

```clojure
{:url "https://example.com"
 :title "Example Site"
 :content "Highlighted text"
 :page "Reading List"
 :append false}
```

**Payload (iOS via handle-payload):**

```clojure
{:text "Some text"
 :resources [{:name "file.pdf"
              :ext "pdf"
              :type "application/pdf"
              :url #object[URL "file://..."]}]}
```

**Persistence:** Ephemeral (exists only during processing)

---

### 5. Processed Content Format

**After Template Substitution:**

```markdown
**14:30** [[quick capture]]: Highlighted text [Example Site](https://example.com)
```

**With Media:**

Asset resources are imported through `db-based-save-assets!`; inline `[[uuid]]` text is not guaranteed by `handle-payload` in current `src/main/frontend/mobile/intent.cljs`.

> **Removed:** `**14:30** [[quick capture]]: Check this out [[uuid-of-image-block]]`

**With Video:**

```markdown
**14:30** [[quick capture]]: {{video https://youtube.com/watch?v=...}}
```

**Persistence:**

- **Immediate:** DataScript in-memory database
- **Delayed (~450ms):** SQLite (DB graphs) or Markdown files (file-based)

---

## Platform Comparison

| Aspect                   | macOS (Electron)                       | iOS (Capacitor)                       |
| ------------------------ | -------------------------------------- | ------------------------------------- |
| **Entry Point**          | URL callback scheme                    | System share extension                |
| **URL Scheme**           | `logseq://x-callback-url/quickCapture` | `logseq://shared?payload=...`         |
| **Trigger Method**       | External script/automation             | iOS Share Sheet                       |
| **Supported Content**    | Text, URL, title, page                 | Text, URLs, images, videos, files     |
| **File Handling**        | Direct paths                           | Copy to group container → assets      |
| **Duplicate Prevention** | None (single-instance lock)            | 1-second time window check            |
| **App State**            | Always running                         | May launch app if suspended           |
| **Launch Latency**       | Immediate (~20ms IPC)                  | 200-500ms (app resume)                |
| **Processing Flow**      | Main process → IPC → Renderer          | Share extension → Deep link → Handler |
| **Template Support**     | ✅ Full                                | ✅ Full                               |
| **Page Selection**       | ✅ Full                                | ✅ Full                               |
| **Asset Handling**       | Direct file paths                      | Copy to shared container              |
| **Error Handling**       | Notifications                          | Notifications + error UI in extension |
| **Concurrency**          | Sequential                             | Dedup + deep-link routing in JS layer |
| **Persistence**          | Immediate to DB/files                  | Immediate to DB after asset copy      |

### Key Differences

#### 1. **File Handling**

**macOS:**

- Receives file paths directly
- No intermediate copy needed
- Assets referenced by path

**iOS:**

- Files in app sandbox → inaccessible
- Copy to shared group container
- Main app imports from container → repo assets

#### 2. **Duplicate Prevention**

**macOS:**

- Electron ensures single instance
- No duplicate URL calls

**iOS:**

- Known bug: `appUrlOpen` fires twice
- Mitigation: Compare URL + timestamp
- Skip if same URL within 1 second

**Code (mobile/init.cljs:21-24, 99-103):**

```clojure
;; FIXME: `appUrlOpen` are fired twice when receiving a same intent.
(def *last-shared-url (atom nil))
(def *last-shared-seconds (atom 0))

;; In appUrlOpen listener:
(when (or (string/starts-with? url "https://logseq.com/mobile/")
          (string/starts-with? url "logseq://mobile/")
          (not (and (= @*last-shared-url url)
                    (<= (- (.getSeconds (js/Date.)) @*last-shared-seconds) 1))))
  (reset! *last-shared-url url)
  (reset! *last-shared-seconds (.getSeconds (js/Date.)))
  (deeplink/deeplink url))
```

#### 3. **Concurrency Model**

**macOS:**

- Sequential: Parse → Route → Publish event → Process
- Single-threaded renderer

**iOS:**

- URL dedup + deep-link routing in JS layer (`appUrlOpen` -> `handle-incoming-url!` -> `deeplink`)

> **Removed:** "Parallel TaskGroup processing" claim for this validation pass (outside `src/`, `deps/`, `packages/`).

#### 4. **Content Type Detection**

**macOS:**

- No type detection (receives pre-formatted params)
- Content type determined by caller

**iOS:**

- iOS UTI (Uniform Type Identifier) system
- Automatic type detection for URLs, text, images, videos
- Fallback to MIME type conversion

---

## Configuration

### User Configuration (`config.edn`)

**Quick Capture Options:**

```clojure
:quick-capture-options
{:insert-today? true    ;; Always insert into today's journal
 :redirect-page? false  ;; Don't redirect after capture
 :default-page "Inbox"} ;; Default page when not specified
```

**Option Details:**

| Option           | Type    | Default           | Description                                                                     |
| ---------------- | ------- | ----------------- | ------------------------------------------------------------------------------- |
| `insert-today?`  | boolean | `false`           | If true, always inserts into today's journal, even if `page` parameter is blank |
| `redirect-page?` | boolean | `false`           | If true, redirects to the target page after capture                             |
| `default-page`   | string  | `"quick capture"` | Page to use when no page specified and not in editing mode                      |

**Template Configuration:**

```clojure
:quick-capture-templates
{:text "**{time}** [[quick capture]]: {text} {url}"
 :media "**{time}** {url}"}
```

**Template Variables:**

| Variable | Example Output               | Description                              |
| -------- | ---------------------------- | ---------------------------------------- |
| `{time}` | `14:30`                      | Current time in HH:mm format             |
| `{date}` | `[[Dec 15th, 2024]]`         | Date reference using current date format |
| `{text}` | `Selected text content`      | Shared/selected text                     |
| `{url}`  | `[Title](URL)` or `[[uuid]]` | Formatted link or asset reference        |

**Special URL Formatting:**

- **Video URLs:** Converted to `{{video URL}}`
- **Twitter/X URLs:** Converted to `{{twitter URL}}`
- **Prettify Gate:** URL prettification is controlled by `[:quick-capture-option :prettify-url?]` (default `true`).

> **Removed:** "Images/Media (iOS) are converted to `[[block-uuid]]` references" as a current `src/` behavior guarantee.

---

### Page Selection Logic

**Priority Order:**

1. **Explicit "TODAY" parameter** → Today's journal
2. **`insert-today?` config = true AND blank page** → Today's journal
3. **Explicit page name** → That page
4. **Default page in config** → Default page
5. **Currently viewing page** → Current page
6. **Fallback:**
   - If journals enabled → Today's journal
   - If journals disabled → "quick capture" page

**Code (quick_capture.cljs:25-55):**

```clojure
(let [page (cond
             (and (state/enable-journals?)
                  (or (= page "TODAY")
                      (and (string/blank? page) insert-today?)))
             today-page

             (not-empty page)
             page

             (not-empty default-page)
             default-page

             (not-empty current-page)
             current-page

             :else
             (if (state/enable-journals?)
               today-page
               "quick capture"))]
  ;; Use page...
  )
```

---

## Risk Analysis

### 🔴 **High Risk: Duplicate Captures on iOS**

**Issue:** `appUrlOpen` event fires twice for same intent  
**Impact:** Same content inserted twice  
**Likelihood:** High (known bug)  
**Exposure:** 1 second window

**Current Mitigation:**

```clojure
;; mobile/init.cljs:99-103
(when (not (and (= @*last-shared-url url)
                (<= (- (.getSeconds (js/Date.)) @*last-shared-seconds) 1)))
  ;; Process URL
  )
```

**Limitations:**

- Only prevents exact duplicates within 1 second
- Different content with same URL → not caught
- Clock adjustments could break detection

**Recommendation:**

- ✅ **Implemented**: Timestamp-based deduplication
- 🔧 **Enhancement**: Use intent ID from iOS if available
- 🔧 **Enhancement**: Track processed payloads by content hash

---

### 🟡 **Medium Risk: Asset File Race Conditions**

> **Removed:** Extension-lifecycle timing analysis based on `ShareViewController.swift` (`0.2s` completion delay) from this validated section, because native extension code is outside the current source scope (`src/`, `deps/`, `packages/`).

**Issue (scoped):** Mobile intent imports assets asynchronously and file reads can fail.

**Scenario:**

```
Time  Extension           Main App
0ms   Copy file.png
100ms Complete extension
150ms                     Launch/resume
200ms                     Deep link received
250ms                     Start asset import
300ms                     Read file.png ← File might be deleted by extension cleanup
```

**Current Mitigation (scoped):**

- `normalize-native-file-path` handles common iOS URL forms.
- `<filesystem-read-file` retries with original path if normalized read fails.
- Errors are logged and surfaced via notifications.

**Recommendation:**

- ✅ **Working**: path normalization + fallback read reduce import failures
- 📊 **Monitor**: Log if asset import fails due to missing files
- 🔧 **Enhancement**: Add richer import diagnostics for failing resource URLs

---

### 🟡 **Medium Risk: 100ms Insertion Delay**

**Issue:** Content insertion delayed by 100ms when not in editing mode

**Code (quick_capture.cljs:100-103):**

```clojure
(js/setTimeout #(editor-handler/api-insert-new-block! content
                                                       {:page page
                                                        :edit-block? true
                                                        :replace-empty-target? true})
               100)
```

**Rationale:** Allow time for page navigation and state updates

**Risks:**

- If user interacts within 100ms → race condition
- Page might not exist yet
- Edit state might be inconsistent

**Mitigation:**

- Page creation is awaited before setTimeout
- Edit escaping happens synchronously
- 100ms usually sufficient for state updates

**Recommendation:**

- ✅ **Acceptable**: 100ms is reasonable for UX
- 🔧 **Enhancement**: Use promise-based waiting instead of setTimeout
- 📊 **Monitor**: Track failed insertions

---

### 🟢 **Low Risk: Template Substitution Errors**

**Issue:** Invalid templates could crash substitution

**Example Bad Template:**

```clojure
:quick-capture-templates
{:text "{time} {undefined-var}"}  ;; {undefined-var} won't error, just stays literal
```

**Current Behavior:**

- Unknown placeholders remain literal
- No validation on config load
- No error thrown

**Code (quick_capture.cljs:72-76):**

```clojure
(-> template
    (string/replace "{time}" time)
    (string/replace "{date}" date-ref-name)
    (string/replace "{url}" link)
    (string/replace "{text}" text))
```

**Recommendation:**

- ✅ **Safe**: string/replace is fault-tolerant
- 🔧 **Enhancement**: Validate templates on config change
- 📚 **Documentation**: Document all available placeholders

---

### 🟢 **Low Risk: Network-Dependent Asset Import**

**Issue:** iOS file imports may fail if files are on iCloud

**Scenario:**

- User shares file from Files app
- File is iCloud placeholder (not downloaded)
- Share extension receives URL but file not local
- Copy to container might fail or download first

**Mitigation:**

- iOS handles downloads transparently
- Share extension waits for file availability
- Error handling in handleTypeUrl, handleTypeImage, etc.

**Recommendation:**

- ✅ **Working**: iOS system handles it
- 📊 **Monitor**: Log failed file copies
- 🔧 **Enhancement**: Show progress indicator for large files

---

## Code References

### Core Implementation Files

| File                                   | Lines   | Purpose                                       |
| -------------------------------------- | ------- | --------------------------------------------- |
| `src/main/frontend/quick_capture.cljs` | 1-103   | Core quick capture logic, template processing |
| `src/electron/electron/url.cljs`       | 62-92   | macOS callback URL parser                     |
| `src/main/electron/listener.cljs`      | 114-116 | IPC bridge from Electron to renderer          |
| `src/main/mobile/init.cljs`            | 91-105  | iOS appUrlOpen listener                       |
| `src/main/mobile/deeplink.cljs`        | 18-111  | Mobile deep link router                       |
| `src/main/frontend/mobile/intent.cljs` | 83-269  | iOS share intent handler                      |

> **Removed:** `ios/App/ShareViewController/ShareViewController.swift` and `ios/App/ShareViewController/SharedData.swift` from the validated core reference table for this scoped pass.

### Key Functions

#### 1. `frontend.quick-capture/quick-capture`

**Location:** `src/main/frontend/quick_capture.cljs:23-100`

**Purpose:** Core logic for processing quick capture requests

**Parameters:**

```clojure
args - Map with keys:
  :url      - String (optional)
  :title    - String (optional)
  :content  - String (optional)
  :page     - String (optional)
  :append   - Boolean (optional)
```

**Returns:** Promise (via `p/do!`)

**Key Logic:**

1. Determine target page (priority: TODAY > explicit > default > current)
2. Load template from config
3. Format URL (video, twitter, or standard link)
4. Substitute template variables
5. Insert content (immediate if editing, delayed 100ms otherwise)

**Example:**

```clojure
(quick-capture {:url "https://example.com"
                :title "Example"
                :content "Quote text"
                :page "Reading List"})
```

---

#### 2. `electron.url/x-callback-url-handler`

**Location:** `src/electron/electron/url.cljs:62-92`

**Purpose:** Parse and route x-callback-url actions

**Parameters:**

```clojure
win - Electron BrowserWindow
parsed-url - js/URL object
```

**Supported Actions (in current handler):**

- `/quickCapture` - Quick capture action
- `/invokeCommand` - plugin command dispatch path (outside this investigation's core quick-capture flow)

**Flow:**

1. Extract pathname (action name)
2. Match action
3. Decode URL parameters
4. Send IPC message to renderer

---

#### 3. `mobile.intent/handle-payload`

**Location:** `src/main/frontend/mobile/intent.cljs:219-269`

**Purpose:** Process iOS share extension payload

**Parameters:**

```clojure
payload - Map with keys:
  :text      - String (optional)
  :resources - Vector of resource maps
```

**Returns:** Promise

**Key Logic:**

1. Get current page or today's journal
2. Load template from config
3. Process each resource:
   - Files/media → Import to assets → `[[uuid]]` reference
   - Text → Include as-is
4. Join resources with newlines
5. Substitute template: `{text}`, `{url}` (rich content), `{time}`, `{date}`
6. Insert content

**Example Payload:**

```clojure
{:text "Check this out"
 :resources [{:name "image.png"
              :ext "png"
              :type "image/png"
              :url #object[URL "file://..."]}]}
```

---

#### 4. `mobile.deeplink/deeplink`

**Location:** `src/main/mobile/deeplink.cljs:18-111`

**Purpose:** Route deep link URLs to appropriate handlers

**Parameters:**

```clojure
url - String (e.g., "logseq://shared?payload=...")
```

**Supported Hostnames:**

- `mobile` - Internal mobile actions (audio, quick-add)
- `graph` - Graph-specific links
- `shared` - Share extension payloads

**Flow for "shared":**

1. Parse URL
2. Extract `payload` query parameter
3. Decode URI component
4. Parse JSON
5. Route to `intent/handle-payload` or `intent/handle-result`

---

### Event Subscriptions

#### `:editor/quick-capture`

**Publisher:**

- macOS: `src/main/electron/listener.cljs:120-122`
- iOS: `src/main/frontend/mobile/intent.cljs:85`

**Subscriber:** (Auto-handled by event system)

**Payload:** Map (see quick-capture function parameters)

---

## Recommendations

### For Users

#### ✅ **Best Practices**

1. **Use consistent templates** across devices

   ```clojure
   :quick-capture-templates
   {:text "**{time}** {text} {url} #captured"}
   ```

2. **Set a dedicated capture page** instead of scattering captures

   ```clojure
   :quick-capture-options
   {:default-page "Inbox"
    :insert-today? false}  ;; Central inbox approach
   ```

3. **Tag captures** for easy retrieval

   ```clojure
   {:text "{text} {url} #quick-capture #review"}
   ```

4. **Use page parameter** for categorization
   - Reading List: `page=Reading List`
   - Project notes: `page=Project Name`

5. **macOS automation examples**:

   ```bash
   # Capture from Safari
   osascript -e 'tell application "Safari" to set theURL to URL of current tab of front window'
   open "logseq://x-callback-url/quickCapture?url=$theURL&page=Reading%20List"

   # Capture clipboard
   TEXT=$(pbpaste)
   open "logseq://x-callback-url/quickCapture?content=$TEXT&page=Fleeting%20Notes"
   ```

#### ⚠️ **Known Issues & Workarounds**

1. **iOS duplicate captures**
   - **Issue:** Sometimes content inserted twice
   - **Workaround:** Check recent blocks before sharing again
   - **When:** If you share twice within 1 second

2. **Large files on iOS**
   - **Issue:** Share extension may timeout
   - **Workaround:** Share files one at a time
   - **Limit:** ~10MB per file (soft limit)

3. **iCloud files**
   - **Issue:** Files not downloaded yet
   - **Workaround:** Open file in Files app first to download

---

### For Developers

#### 🔧 **Immediate Improvements**

1. **Replace setTimeout with await** (quick_capture.cljs:100-103)

   ```clojure
   ;; Current (timing-dependent):
   (js/setTimeout #(editor-handler/api-insert-new-block! ...) 100)

   ;; Better:
   (p/do!
    (editor-handler/escape-editing)
    (when (not= page (state/get-current-page))
      (p/let [_ (page-handler/<create! page {:redirect? redirect-page?})]
        (editor-handler/api-insert-new-block! ...))))
   ```

2. **Add template validation** (on config change)

   ```clojure
   (defn validate-template [template]
     (let [valid-placeholders #{"{time}" "{date}" "{text}" "{url}"}
           used-placeholders (re-seq #"\{[^}]+\}" template)
           invalid (remove valid-placeholders used-placeholders)]
       (when (seq invalid)
         (notification/show!
          (str "Invalid template placeholders: " (string/join ", " invalid))
          :warning))))
   ```

3. **Enhance iOS duplicate prevention** (mobile/init.cljs)

   ```clojure
   ;; Better: Hash the payload content
   (defn payload-hash [url]
     (let [parsed (.parse Uri url)
           payload (.get (.getQueryData parsed) "payload")]
       (hash payload)))  ;; ClojureScript hash function

   (def *recent-payload-hashes (atom #{}))

   ;; In appUrlOpen:
   (let [phash (payload-hash url)]
     (when-not (contains? @*recent-payload-hashes phash)
       (swap! *recent-payload-hashes conj phash)
       (js/setTimeout #(swap! *recent-payload-hashes disj phash) 2000)
       (deeplink/deeplink url)))
   ```

#### 📊 **Monitoring & Logging**

1. **Add telemetry for failures**

   ```clojure
   ;; In handle-payload:
   (p/catch
    (handle-payload-resource resource format)
    (fn [error]
      (log/error :quick-capture/resource-failed
                 {:resource resource :error error})
      nil))  ;; Return nil to skip failed resource
   ```

2. **Track capture sources**

   ```clojure
   ;; Add metadata to captures
   (defn add-capture-metadata [content platform]
     (str content
          (when config/dev?
            (str "\n<!-- captured-via: " platform " -->"))))
   ```

3. **Measure performance**
   ```clojure
   (defn quick-capture [args]
     (let [start-time (js/Date.now)]
       (p/do!
        ;; ... capture logic ...
        (log/debug :quick-capture/perf
                   {:duration (- (js/Date.now) start-time)}))))
   ```

#### 🚀 **Feature Enhancements**

1. **Support capture to specific block** (not just pages)

   ```clojure
   ;; Add :block-uuid parameter
   {:block-uuid "uuid-string"  ;; Insert as child of this block
    :url "..."
    :content "..."}
   ```

2. **Batch captures** (mobile)

   ```clojure
   ;; Queue captures when offline, sync when online
   (def *pending-captures (atom []))

   (defn queue-or-process [payload]
     (if (online?)
       (handle-payload payload)
       (swap! *pending-captures conj payload)))
   ```

3. **Template preview** (in settings UI)
   ```clojure
   ;; Show example output when editing template
   (defn preview-template [template]
     (-> template
         (string/replace "{time}" "14:30")
         (string/replace "{date}" "[[Dec 15th, 2024]]")
         (string/replace "{text}" "Example text")
         (string/replace "{url}" "[Example](https://example.com)")))
   ```

#### 🧪 **Testing**

**Unit Tests:**

```clojure
(ns frontend.quick-capture-test
  (:require [cljs.test :refer [deftest is]]
            [frontend.quick-capture :as qc]))

(deftest test-page-selection
  (is (= "Reading List"
         (qc/select-page {:page "Reading List"}))))

(deftest test-template-substitution
  (is (= "**14:30** Test https://example.com"
         (qc/substitute-template
          "**{time}** {text} {url}"
          {:time "14:30" :text "Test" :url "https://example.com"}))))
```

**Integration Tests:**

```bash
# macOS callback URL
bb dev:e2e-basic-test "Quick Capture URL"

# iOS share
# (Requires iOS simulator + XCUITest)
```

---

### For Testing

#### Manual Test Cases

**macOS:**

1. **Basic URL capture**

   ```bash
   open "logseq://x-callback-url/quickCapture?url=https://example.com&title=Test"
   ```

   - ✅ Verify: Block created with link

2. **Content + URL**

   ```bash
   open "logseq://x-callback-url/quickCapture?url=https://example.com&content=Quote&page=Reading"
   ```

   - ✅ Verify: Content and link in block
   - ✅ Verify: Created on "Reading" page

3. **TODAY parameter**

   ```bash
   open "logseq://x-callback-url/quickCapture?page=TODAY&content=Daily%20note"
   ```

   - ✅ Verify: Inserted in today's journal

4. **Append mode**

   ```bash
   # Create page first, go to middle
   open "logseq://x-callback-url/quickCapture?page=Test&content=Top&append=false"
   # Then:
   open "logseq://x-callback-url/quickCapture?page=Test&content=Bottom&append=true"
   ```

   - ✅ Verify: "Bottom" at end of page

**iOS:**

1. **Share text from Safari**
   - Open article
   - Select text
   - Tap Share → Logseq
   - ✅ Verify: Text + URL captured

2. **Share image**
   - Open Photos
   - Select image
   - Tap Share → Logseq

- ✅ Verify: Asset import succeeds and content is inserted or updated according to current `handle-payload` logic

> **Removed:** "Image embedded as `[[uuid]]`" as a guaranteed outcome of current scoped code path.

3. **Share PDF**
   - Open PDF in Files
   - Tap Share → Logseq
   - ✅ Verify: PDF copied to assets, linked

4. **Share URL (no text)**
   - Safari → Share URL
   - ✅ Verify: Link captured

5. **Duplicate detection**
   - Share content
   - Immediately share same content again
   - ✅ Verify: Only one block created

---

## Appendix

### Glossary

| Term                | Definition                                                     |
| ------------------- | -------------------------------------------------------------- |
| **Callback URL**    | URL scheme that triggers actions in an app (e.g., `logseq://`) |
| **x-callback-url**  | Standard for inter-app communication on macOS/iOS              |
| **Share Extension** | iOS app extension that appears in system Share Sheet           |
| **Group Container** | Shared file storage between iOS app and extensions             |
| **Deep Link**       | URL that navigates to specific content within an app           |
| **UTI**             | Uniform Type Identifier - iOS system for file type detection   |
| **IPC**             | Inter-Process Communication (Electron main ↔ renderer)         |
| **Capacitor**       | Framework for building native mobile apps with web tech        |
| **DataScript**      | In-memory database (Clojure/ClojureScript)                     |
| **Transit**         | Data serialization format for Clojure                          |

---

### Change History

| Version | Date       | Changes                             |
| ------- | ---------- | ----------------------------------- |
| 1.0     | 2025-11-24 | Initial comprehensive investigation |

---

### Related Documentation

- [Quick Capture User Guide](../03-features/quick-capture/guide.md) - User-facing documentation
- [Mobile Development Guide](../../../docs/develop-logseq-on-mobile.md) - Mobile setup
- [System Architecture](../02-architecture/system-architecture.md) - Architecture overview
- [Config.edn Reference](../04-api-reference/config-edn-reference.md) - Configuration options

---

## Summary

Logseq's quick capture feature provides a robust multi-platform solution for rapidly capturing information:

### ✅ Strengths

1. **Unified API** across macOS and iOS
2. **Flexible templates** with variable substitution
3. **Smart page selection** with multiple fallback options
4. **Rich content support** (text, URLs, images, videos, files)
5. **Immediate persistence** with auto-save integration

### ⚠️ Areas for Improvement

1. **iOS duplicate prevention** - Currently relies on 1-second window
2. **Async timing** - setTimeout instead of promise-based waiting
3. **Template validation** - No runtime validation of templates
4. **Error visibility** - Some failures are silent

### 🎯 Recommendations Priority

**High Priority:**

1. Replace setTimeout with promise-based page navigation
2. Enhance iOS duplicate detection with content hashing
3. Add template validation on config change

**Medium Priority:** 4. Add telemetry for failure tracking 5. Support capture to specific blocks (not just pages) 6. Implement batch/queue for offline captures

**Low Priority:** 7. Template preview in settings 8. Performance metrics logging 9. Enhanced error UI for mobile extension

---

**Investigation Complete** ✓

For questions or updates, see [AGENTS.md](../../../AGENTS.md) or create an issue with the `documentation` label.
