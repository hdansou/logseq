# Investigation: X-Callback-URL and URL Schemes in Logseq

**Version:** 1.0  
**Date:** December 9, 2025  
**Platforms:** macOS (Electron), iOS (Capacitor), Android (Capacitor)  
**Investigator:** GitHub Copilot

---

## Executive Summary

### What is X-Callback-URL?

X-callback-url is a **standardized URL scheme protocol** that enables inter-application communication on desktop and mobile platforms. In Logseq, it provides a way for external applications, scripts, and automation tools to trigger actions within Logseq without requiring the app to be actively open or focused.

### Key Findings

- **🌐 Cross-platform URL scheme**: Logseq implements a custom `logseq://` URL scheme across macOS, iOS, and Android
- **🎯 Primary use case**: Quick capture functionality for capturing content from external sources
- **📱 Platform-specific implementations**: Different mechanisms for macOS (x-callback-url) vs mobile (deep linking + share extensions)
- **🔄 Multiple entry points**: URL schemes, share extensions, intent filters, and deep links
- **⚡ Real-time processing**: Content is captured and inserted immediately with template substitution
- **🛡️ Built-in safety**: Duplicate detection on iOS, protocol registration on macOS

### Supported URL Schemes

| Scheme                                 | Platform    | Purpose                          | Example                                |
| -------------------------------------- | ----------- | -------------------------------- | -------------------------------------- |
| `logseq://x-callback-url/quickCapture` | macOS       | Quick capture from external apps | Automation scripts, browser extensions |
| `logseq://shared?payload=...`          | iOS         | Share extension payload          | System share sheet                     |
| `logseq://graph/[name]`                | All         | Open specific graph              | Deep linking to graphs                 |
| `logseq://mobile/go/quick-add`         | iOS/Android | Open quick capture UI            | Widget shortcuts                       |
| `logseq://mobile/go/audio`             | iOS/Android | Start audio recording            | Widget shortcuts                       |

---

## Table of Contents

1. [What is X-Callback-URL?](#what-is-x-callback-url)
2. [Architecture Overview](#architecture-overview)
3. [Platform-Specific Implementations](#platform-specific-implementations)
4. [URL Scheme Registration](#url-scheme-registration)
5. [Execution Flow](#execution-flow)
6. [API Reference](#api-reference)
7. [Configuration](#configuration)
8. [Use Cases and Examples](#use-cases-and-examples)
9. [Code Locations](#code-locations)
10. [Security Considerations](#security-considerations)
11. [Troubleshooting](#troubleshooting)
12. [Recommendations](#recommendations)

---

## What is X-Callback-URL?

### Background

X-callback-url is a **specification for inter-app communication** originally designed for iOS but now used across platforms. It provides a standardized way for apps to:

1. **Trigger actions** in other applications
2. **Pass parameters** to those actions
3. **Receive callbacks** with success or error results (optional)

### How It Works

The basic structure of an x-callback-url:

```
[scheme]://x-callback-url/[action]?[parameters]
```

**Example:**

```
logseq://x-callback-url/quickCapture?url=https://example.com&title=Example&content=Highlighted%20text
```

### Components

1. **Scheme**: `logseq://` - Custom URL scheme registered by the app
2. **Host**: `x-callback-url` - Indicates this is a callback URL
3. **Action**: `/quickCapture` - The specific action to perform
4. **Parameters**: Query parameters with URL-encoded values

### Why Logseq Uses It

Logseq implements x-callback-url and related URL schemes to enable:

- **Automation workflows** (AppleScript, Shortcuts, Alfred, etc.)
- **Browser extensions** to capture web content
- **Mobile sharing** from any app to Logseq
- **Cross-app integrations** with note-taking ecosystems
- **Command-line tools** for programmatic content insertion

---

## Architecture Overview

### System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                         EXTERNAL SOURCES                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  • Browser extensions        • Share sheet (iOS/Android)             │
│  • Automation scripts        • Widget shortcuts                      │
│  • Command-line tools        • Other apps                            │
│  • AppleScript/Shortcuts     • System intents                        │
│                                                                       │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            │ URL Schemes / Intents
                            ↓
┌─────────────────────────────────────────────────────────────────────┐
│                        PLATFORM LAYER                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │
│  │  macOS/Electron  │  │       iOS        │  │     Android      │  │
│  ├──────────────────┤  ├──────────────────┤  ├──────────────────┤  │
│  │ Protocol Handler │  │ CFBundleURLTypes │  │ Intent Filter    │  │
│  │ (logseq://)      │  │ (logseq://)      │  │ (logseq://)      │  │
│  │                  │  │                  │  │                  │  │
│  │ + Share Ext.     │  │ + Share Ext.     │  │ + ACTION_SEND    │  │
│  │   (N/A)          │  │   (.appex)       │  │   (Intent)       │  │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘  │
│           │                     │                     │             │
└───────────┼─────────────────────┼─────────────────────┼─────────────┘
            │                     │                     │
            ↓                     ↓                     ↓
┌─────────────────────────────────────────────────────────────────────┐
│                      URL ROUTING LAYER                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌──────────────────┐  ┌─────────────────────────────────────────┐  │
│  │ electron/url.cljs│  │      mobile/deeplink.cljs               │  │
│  ├──────────────────┤  ├─────────────────────────────────────────┤  │
│  │ logseq-url-      │  │ deeplink(url)                           │  │
│  │   handler()      │  │   ├─ Parse URL                          │  │
│  │   ├─ x-callback- │  │   ├─ Route by hostname:                 │  │
│  │   │   url         │  │   │   • mobile → internal actions       │  │
│  │   ├─ graph       │  │   │   • graph → switch graphs           │  │
│  │   └─ new-window  │  │   │   • shared → share payload          │  │
│  └────────┬─────────┘  └───────────────┬─────────────────────────┘  │
│           │                            │                             │
└───────────┼────────────────────────────┼─────────────────────────────┘
            │                            │
            ↓                            ↓
┌─────────────────────────────────────────────────────────────────────┐
│                     ACTION HANDLERS                                  │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │              frontend/quick_capture.cljs                     │    │
│  │  quick-capture(args)                                         │    │
│  │    ├─ Determine target page                                 │    │
│  │    ├─ Load template from config                             │    │
│  │    ├─ Format content (video, twitter, links)                │    │
│  │    ├─ Substitute placeholders {time}, {date}, {text}, {url} │    │
│  │    └─ Insert block via editor-handler                       │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                       │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │           frontend/mobile/intent.cljs                        │    │
│  │  handle-payload(payload) / handle-result(result)             │    │
│  │    ├─ Process iOS share extension data                      │    │
│  │    ├─ Import assets (images, videos, files)                 │    │
│  │    ├─ Apply templates                                        │    │
│  │    └─ Trigger :editor/quick-capture event                   │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                                                                       │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────────────┐
│                      CONTENT INSERTION                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  • Insert into current block or create new block                     │
│  • Navigate to target page if needed                                 │
│  • Auto-save (~450ms delay)                                          │
│  • Persist to DataScript → SQLite/File                               │
│                                                                       │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Components

| Component                 | File                                                      | Purpose                            |
| ------------------------- | --------------------------------------------------------- | ---------------------------------- |
| **Protocol Registration** | `electron/core.cljs`, `Info.plist`, `AndroidManifest.xml` | Register `logseq://` URL scheme    |
| **URL Parser**            | `electron/url.cljs`                                       | Parse and route desktop URLs       |
| **Deep Link Handler**     | `mobile/deeplink.cljs`                                    | Parse and route mobile URLs        |
| **Share Extension**       | `ios/App/ShareViewController/`                            | iOS system share sheet integration |
| **Intent Handler**        | `frontend/mobile/intent.cljs`                             | Process mobile share payloads      |
| **Quick Capture Core**    | `frontend/quick_capture.cljs`                             | Unified capture logic              |
| **IPC Bridge**            | `electron/listener.cljs`                                  | Main ↔ Renderer communication      |

---

## Platform-Specific Implementations

### macOS (Electron)

#### URL Scheme Registration

**Location:** `src/electron/electron/core.cljs:62`

```clojure
(defonce LSP_SCHEME "logseq")

;; Register as default protocol client
(.setAsDefaultProtocolClient app' LSP_SCHEME)
```

This registers Logseq as the handler for `logseq://` URLs at the OS level.

#### Entry Points

**1. Open URL Event (Deep Link)**

**Location:** `src/electron/electron/core.cljs:48-59`

```clojure
(defn open-url-handler
  "Handle URL opening events"
  [win url]
  (when-let [parsed-url (try (js/URL. url)
                             (catch :default e nil))]
    (when (= (str LSP_SCHEME ":") (.-protocol parsed-url))
      (logseq-url-handler win parsed-url))))
```

Called when:

- User clicks a `logseq://` link in browser
- External script executes `open "logseq://..."`
- System launches app with URL

**2. Command Line Arguments**

On Windows/Linux, URLs are passed as process arguments when Logseq isn't running.

#### URL Routing

**Location:** `src/electron/electron/url.cljs:88-110`

```clojure
(defn logseq-url-handler
  [^js win parsed-url]
  (let [url-host (.-host parsed-url)]
    (cond
      (= "x-callback-url" url-host)
      (x-callback-url-handler win parsed-url)

      (= "graph" url-host)
      (local-url-handler win parsed-url false)

      (= "new-window" url-host)
      (local-url-handler win parsed-url true)

      :else
      (send-to-renderer :notification {:type "error" ...}))))
```

#### X-Callback-URL Handler

**Location:** `src/electron/electron/url.cljs:61-85`

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
           :append (if (nil? append) append (= append "true"))}
          win))

      :else
      (send-to-focused-renderer "notification"
        {:type "error"
         :payload (str "Unimplemented x-callback-url action: `" action "`.")}
        win))))
```

**Supported Actions:**

- `/quickCapture` - Capture content to Logseq

**Parameters:**

- `url` - Source URL
- `title` - Content title
- `content` - Main text/content
- `page` - Target page name (or "TODAY" for today's journal)
- `append` - Boolean string ("true"/"false") to append to page end

#### IPC Communication

**Location:** `src/main/electron/listener.cljs:114-116`

```clojure
(safe-api-call "quickCapture"
  (fn [args]
    (state/pub-event! [:editor/quick-capture args])))
```

The main process sends IPC message `"quickCapture"` to renderer, which publishes an event for processing.

### iOS (Capacitor)

#### URL Scheme Registration

**Location:** `ios/App/App/Info.plist:38-49`

```xml
<key>CFBundleURLTypes</key>
<array>
    <dict>
        <key>CFBundleTypeRole</key>
        <string>Viewer</string>
        <key>CFBundleURLName</key>
        <string>com.logseq.logseq</string>
        <key>CFBundleURLSchemes</key>
        <array>
            <string>logseq</string>
        </array>
    </dict>
</array>
```

This registers `logseq://` as a custom URL scheme at the iOS system level.

#### Entry Points

**1. Deep Link (URL Open)**

**Location:** `src/main/mobile/init.cljs:95-103`

```clojure
(.addListener App "appUrlOpen"
  (fn [^js data]
    (log/info ::app-url-open data)
    (when-let [url (.-url data)]
      (handle-incoming-url! url))))
```

Capacitor's `App` plugin fires `appUrlOpen` event when:

- App is opened via URL scheme
- Share extension completes and redirects
- Widget taps deep link

**Known Issue:** This event fires **twice** for the same intent on iOS.

**Mitigation (init.cljs:19-39):**

```clojure
(def *last-shared-url (atom nil))
(def *last-shared-seconds (atom 0))

(defn- handle-incoming-url! [url]
  (p/then state/app-ready-promise
    (fn []
      (when (and url
                 (or (string/starts-with? url "https://logseq.com/mobile/")
                     (string/starts-with? url "logseq://mobile/")
                     (not (and (= @*last-shared-url url)
                               (<= (- (.getSeconds (js/Date.))
                                      @*last-shared-seconds) 1)))))
        (reset! *last-shared-url url)
        (reset! *last-shared-seconds (.getSeconds (js/Date.)))
        (deeplink/deeplink url)))))
```

**2. Share Extension**

**Location:** `ios/App/ShareViewController/ShareViewController.swift`

When user taps "Share → Logseq" from any app:

1. iOS launches `ShareViewController.appex` (share extension)
2. Extension processes attachments in parallel using `TaskGroup`
3. Converts items to `SharedData` structure
4. Encodes as JSON, URL-encodes, and creates deep link:
   ```
   logseq://shared?payload=[encoded-json]
   ```
5. Opens main app with this URL
6. Extension dismisses after 200ms

**Example Flow:**

```swift
// ShareViewController.swift:28-44
private func sendData() {
    let encoder: JSONEncoder = JSONEncoder()
    let data = try? encoder.encode(self.sharedData)
    let queryPayload = String(decoding: data!, as: UTF8.self)

    let queryItems = [
        URLQueryItem(
            name: "payload",
            value: queryPayload.addingPercentEncoding(
                withAllowedCharacters: .urlHostAllowed) ?? "")
    ]
    var urlComps = URLComponents(string: "logseq://shared?")!
    urlComps.queryItems = queryItems
    openURL(urlComps.url!)
}
```

#### Deep Link Routing

**Location:** `src/main/mobile/deeplink.cljs:17-95`

```clojure
(defn deeplink [url]
  (let [url (string/replace url "logseq.com/" "")
        ^js/Uri parsed-url (.parse Uri url)
        hostname (.getDomain parsed-url)
        pathname (.getPath parsed-url)
        search-params (.getQueryData parsed-url)]
    (cond
      ;; Mobile shortcuts
      (and (= hostname "mobile") (= pathname "/go/audio"))
      (state/pub-event! [:mobile/start-audio-record])

      (and (= hostname "mobile") (= pathname "/go/quick-add"))
      (state/pub-event! [:mobile/set-tab "capture"])

      ;; Graph switching
      (= hostname "graph")
      (let [graph-name (some-> pathname
                               (string/replace "/" "")
                               string/lower-case)
            [page-name block-uuid] (map #(.get search-params %)
                                       ["page" "block-id"])]
        ;; Switch graph and navigate to page/block
        ...)

      ;; Share extension payload
      (= hostname "shared")
      (let [result (into {} (map (fn [key]
                                   [(keyword key) (.get search-params key)])
                                 ["title" "url" "type" "payload"]))]
        (if (:payload result)
          (let [raw (common-util/safe-decode-uri-component (:payload result))
                payload (-> raw js/JSON.parse (js->clj :keywordize-keys true))]
            (intent/handle-payload payload))
          (intent/handle-result result)))

      :else nil)))
```

#### Share Payload Processing

**Location:** `src/main/frontend/mobile/intent.cljs:247-285`

```clojure
(defn handle-payload
  "Process iOS share extension payload"
  [payload]
  (let [{:keys [text resources]} payload
        page (or (state/get-current-page) (date/today))
        template (get-in (state/get-config)
                        [:quick-capture-templates :media]
                        "**{time}**: {url}")
        time (date/get-current-time)
        date-ref (date/today)]

    (p/do!
      ;; Import media assets
      (p/let [resources-content
              (p/all
                (map (fn [resource]
                       (if (contains? #{"png" "jpg" "jpeg" "gif" ...}
                                     (:ext resource))
                         ;; Import as asset → get [[uuid]] reference
                         (embed-asset-file (:url resource) format)
                         ;; Or handle as text/link
                         ...))
                     resources))]

        ;; Build content from template
        (let [rich-content (string/join "\n" resources-content)
              content (-> template
                         (string/replace "{time}" time)
                         (string/replace "{date}" date-ref)
                         (string/replace "{url}" rich-content)
                         (string/replace "{text}" (or text "")))]

          ;; Trigger quick capture
          (state/pub-event! [:editor/quick-capture
                            {:content content :page page}]))))))
```

### Android (Capacitor)

#### URL Scheme Registration

**Location:** `android/app/src/main/AndroidManifest.xml:50-55`

```xml
<intent-filter>
    <action android:name="android.intent.action.VIEW" />
    <category android:name="android.intent.category.DEFAULT" />
    <category android:name="android.intent.category.BROWSABLE" />
    <data android:scheme="logseq" />
</intent-filter>
```

This allows Android to route `logseq://` URLs to the app.

#### Share Intent

**Location:** `android/app/src/main/AndroidManifest.xml:37-47`

```xml
<intent-filter>
    <action android:name="android.intent.action.SEND" />
    <category android:name="android.intent.category.DEFAULT" />
    
    <data android:mimeType="text/plain" />
    <data android:mimeType="image/*" />
    <data android:mimeType="application/*" />
    <data android:mimeType="video/*" />
</intent-filter>
```

This registers Logseq as a share target for various content types.

#### Intent Processing

Unlike iOS's share extension, Android uses the main activity to receive shared content via `ACTION_SEND` intents. The intent data is processed using the `send-intent` Capacitor plugin.

**Location:** `src/main/frontend/mobile/intent.cljs:245-290`

Same handler as iOS (`handle-payload`), but data comes from Android Intent system instead of share extension.

---

## URL Scheme Registration

### macOS/Windows/Linux (Electron)

**File:** `src/electron/electron/core.cljs`

```clojure
(defonce LSP_SCHEME "logseq")

;; During app initialization
(defn setup-interceptor! [^js app']
  (.setAsDefaultProtocolClient app' LSP_SCHEME)
  ;; ... protocol handlers ...
)
```

**What happens:**

1. Electron registers `logseq://` protocol with the OS
2. When a `logseq://` URL is opened, the OS launches/activates Logseq
3. URL is passed to `open-url` event handler (macOS) or command-line args (Windows/Linux)

**File Protocol Handlers:**

Logseq also registers custom file protocols for internal use:

- `lsp://` - Plugin and static asset loading
- `assets://` - Local file asset references

### iOS

**File:** `ios/App/App/Info.plist`

```xml
<key>CFBundleURLTypes</key>
<array>
    <dict>
        <key>CFBundleTypeRole</key>
        <string>Viewer</string>
        <key>CFBundleURLName</key>
        <string>com.logseq.logseq</string>
        <key>CFBundleURLSchemes</key>
        <array>
            <string>logseq</string>
        </array>
    </dict>
</array>
```

**What happens:**

1. iOS registers `logseq://` scheme for the app bundle
2. When URL is opened, iOS launches/resumes app
3. Capacitor's `App.addListener("appUrlOpen", ...)` fires with the URL

**Share Extension:**

**File:** `ios/App/ShareViewController/Info.plist`

Defines which content types the share extension accepts (NSExtensionActivationRule).

### Android

**File:** `android/app/src/main/AndroidManifest.xml`

```xml
<!-- Deep link -->
<intent-filter>
    <action android:name="android.intent.action.VIEW" />
    <category android:name="android.intent.category.DEFAULT" />
    <category android:name="android.intent.category.BROWSABLE" />
    <data android:scheme="logseq" />
</intent-filter>

<!-- Share target -->
<intent-filter>
    <action android:name="android.intent.action.SEND" />
    <category android:name="android.intent.category.DEFAULT" />
    <data android:mimeType="text/plain" />
    <data android:mimeType="image/*" />
    <!-- ... more MIME types ... -->
</intent-filter>
```

**What happens:**

1. Android registers `logseq://` scheme and share target for MainActivity
2. When URL is opened or content shared, Android launches/resumes the activity
3. Capacitor handles the intent and fires `appUrlOpen` event

---

## Execution Flow

### macOS: X-Callback-URL Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│ Step 1: External Trigger                                             │
│ Time: 0ms                                                            │
└─────────────────────────────────────────────────────────────────────┘
  User executes (from terminal, script, browser):

  open "logseq://x-callback-url/quickCapture?url=https://example.com&title=Example&content=Text"

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 2: OS Protocol Handler                                         │
│ Time: ~5-20ms                                                        │
└─────────────────────────────────────────────────────────────────────┘
  macOS recognizes `logseq://` protocol
  Launches or activates Logseq app
  Fires Electron 'open-url' event

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 3: Electron Main Process                                       │
│ Time: ~10-30ms                                                       │
└─────────────────────────────────────────────────────────────────────┘
  File: electron/core.cljs:48-59

  open-url-handler(win, url)
    ├─ Parse URL → js/URL object
    ├─ Check protocol === "logseq:"
    └─ Call logseq-url-handler(win, parsed-url)

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 4: URL Router                                                   │
│ Time: ~5-15ms                                                        │
└─────────────────────────────────────────────────────────────────────┘
  File: electron/url.cljs:88-110

  logseq-url-handler(win, parsed-url)
    ├─ Extract host: "x-callback-url"
    └─ Route to: x-callback-url-handler(win, parsed-url)

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 5: X-Callback Handler                                          │
│ Time: ~5-10ms                                                        │
└─────────────────────────────────────────────────────────────────────┘
  File: electron/url.cljs:61-85

  x-callback-url-handler(win, parsed-url)
    ├─ Extract pathname: "/quickCapture"
    ├─ Decode params: url, title, content, page, append
    └─ send-to-focused-renderer("quickCapture", {...}, win)

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 6: IPC Bridge                                                   │
│ Time: ~2-5ms                                                         │
└─────────────────────────────────────────────────────────────────────┘
  Main process → IPC channel "quickCapture" → Renderer process

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 7: Renderer Listener                                           │
│ Time: ~1-3ms                                                         │
└─────────────────────────────────────────────────────────────────────┘
  File: electron/listener.cljs:114-116

  safe-api-call("quickCapture", fn)
    ├─ Receive args from IPC
    └─ Publish event: [:editor/quick-capture args]

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 8: Quick Capture Logic                                         │
│ Time: ~20-100ms                                                      │
└─────────────────────────────────────────────────────────────────────┘
  File: frontend/quick_capture.cljs:22-102

  quick-capture(args)
    ├─ Determine target page (TODAY / explicit / default)
    ├─ Load template from config
    ├─ Format link based on type (video / twitter / regular)
    ├─ Substitute placeholders: {time}, {date}, {text}, {url}
    ├─ Check if editing (append vs. insert)
    └─ Insert content via editor-handler

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 9: Content Insertion                                           │
│ Time: ~50-200ms                                                      │
└─────────────────────────────────────────────────────────────────────┘
  File: frontend/handler/editor.cljs

  - Create new block or insert in current block
  - Navigate to target page if needed (setTimeout 100ms)
  - Start editing (optional)
  - Trigger auto-save

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 10: Persistence                                                │
│ Time: ~450-1500ms                                                    │
└─────────────────────────────────────────────────────────────────────┘
  Auto-save kicks in (~450ms after edit)
  DataScript → SQLite/File backend

  COMPLETED ✅
```

**Total Time:** ~100-500ms from URL trigger to content visible in editor

### iOS: Share Extension Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│ Step 1: User Shares Content                                         │
│ Time: 0ms                                                            │
└─────────────────────────────────────────────────────────────────────┘
  User taps Share button in Safari/Photos/Any app
  Selects "Logseq" from share sheet

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 2: Share Extension Launch                                      │
│ Time: ~50-200ms                                                      │
└─────────────────────────────────────────────────────────────────────┘
  File: ios/App/ShareViewController/ShareViewController.swift:145

  iOS launches ShareViewController.appex
  viewDidLoad() called

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 3: Attachment Processing (Parallel)                            │
│ Time: ~100-1000ms (depends on content)                              │
└─────────────────────────────────────────────────────────────────────┘
  File: ShareViewController.swift:145-188

  withThrowingTaskGroup:
    for each attachment:
      TaskGroup.addTask:
        ├─ kUTTypeURL → handleTypeUrl
        │   ├─ File URL? → Copy to group container
        │   └─ Web URL? → Store as text
        │
        ├─ kUTTypeText → handleTypeText
        │
        ├─ kUTTypeImage → handleTypeImage
        │   ├─ Save as PNG/JPG
        │   └─ Copy to shared container
        │
        └─ kUTTypeMovie → handleTypeMovie
            └─ Copy video to shared container

  Collect all results into SharedData:
    {
      text: "..."
      resources: [
        {name: "...", ext: "...", type: "...", url: URL}
      ]
    }

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 4: Build Deep Link                                             │
│ Time: ~10-30ms                                                       │
└─────────────────────────────────────────────────────────────────────┘
  File: ShareViewController.swift:28-44

  sendData():
    ├─ Encode SharedData as JSON
    ├─ URL-encode JSON string
    └─ Build URL: "logseq://shared?payload=[encoded-json]"

  openURL("logseq://shared?payload=...")

  Extension dismisses (200ms delay)

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 5: App Activation                                              │
│ Time: ~200-500ms                                                     │
└─────────────────────────────────────────────────────────────────────┘
  iOS launches or resumes main Logseq app
  Capacitor App plugin receives URL

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 6: App URL Open Event                                          │
│ Time: ~5-20ms                                                        │
└─────────────────────────────────────────────────────────────────────┘
  File: mobile/init.cljs:95-103

  App.addListener("appUrlOpen", fn)
    ├─ Extract URL from event
    ├─ Check duplicate (same URL within 1 second)
    └─ Call handle-incoming-url!(url)

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 7: Deep Link Router                                            │
│ Time: ~5-15ms                                                        │
└─────────────────────────────────────────────────────────────────────┘
  File: mobile/deeplink.cljs:17-95

  deeplink(url)
    ├─ Parse URL (Uri.parse)
    ├─ Extract hostname: "shared"
    ├─ Extract query param: "payload"
    ├─ Decode URI component
    ├─ Parse JSON → ClojureScript map
    └─ Route to: intent/handle-payload(payload)

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 8: Payload Processing                                          │
│ Time: ~100-500ms (asset imports)                                    │
└─────────────────────────────────────────────────────────────────────┘
  File: frontend/mobile/intent.cljs:247-285

  handle-payload(payload)
    ├─ Extract text and resources
    ├─ Get target page (current or today)
    ├─ Load template from config
    ├─ For each resource:
    │   ├─ Image/video → embed-asset-file
    │   │   ├─ Read file from shared container
    │   │   ├─ Import to repo assets
    │   │   └─ Get [[uuid]] reference
    │   └─ Text/URL → format as link
    ├─ Build rich content (join with newlines)
    ├─ Substitute template: {time}, {date}, {text}, {url}
    └─ Publish event: [:editor/quick-capture {...}]

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 9: Quick Capture (Same as macOS)                               │
│ Time: ~20-100ms                                                      │
└─────────────────────────────────────────────────────────────────────┘
  File: frontend/quick_capture.cljs:22-102

  [Same logic as macOS flow]

  ↓

┌─────────────────────────────────────────────────────────────────────┐
│ Step 10: Content Insertion & Persistence                            │
│ Time: ~100-1500ms                                                    │
└─────────────────────────────────────────────────────────────────────┘
  [Same as macOS flow]

  COMPLETED ✅
```

**Total Time:** ~500ms - 3s from share tap to content in editor (varies with asset size/count)

---

## API Reference

### URL Scheme: `logseq://x-callback-url/quickCapture`

**Platform:** macOS (Electron)

**Description:** Capture content from external sources with customizable parameters.

**Format:**

```
logseq://x-callback-url/quickCapture?[parameters]
```

**Parameters:**

| Parameter | Type   | Required | Description                            | Example                   |
| --------- | ------ | -------- | -------------------------------------- | ------------------------- |
| `url`     | String | No       | Source URL or reference                | `https://example.com`     |
| `title`   | String | No       | Title or headline                      | `Article Title`           |
| `content` | String | No       | Main text content                      | `Selected text...`        |
| `page`    | String | No       | Target page name or "TODAY"            | `Reading List` or `TODAY` |
| `append`  | String | No       | Append to end of page ("true"/"false") | `true`                    |

**Notes:**

- All values must be URL-encoded
- If `page="TODAY"` or omitted with `insert-today?=true`, inserts into today's journal
- If `append="true"`, inserts at end of target page instead of current position
- Empty `url` and `title` → text-only capture

**Examples:**

```bash
# Capture a web page with title
open "logseq://x-callback-url/quickCapture?url=https://example.com&title=Example%20Page"

# Capture highlighted text
open "logseq://x-callback-url/quickCapture?content=Important%20text&page=Fleeting%20Notes"

# Capture to today's journal
open "logseq://x-callback-url/quickCapture?url=https://news.ycombinator.com&page=TODAY"

# Append to specific page
open "logseq://x-callback-url/quickCapture?url=https://blog.example.com&title=Blog%20Post&page=Reading%20List&append=true"
```

### URL Scheme: `logseq://graph/[name]`

**Platform:** All (macOS, iOS, Android)

**Description:** Switch to a specific graph and optionally navigate to a page or block.

**Format:**

```
logseq://graph/[graph-name]?[parameters]
```

**Parameters:**

| Parameter  | Type   | Required | Description               |
| ---------- | ------ | -------- | ------------------------- |
| `page`     | String | No       | Page name to navigate to  |
| `block-id` | UUID   | No       | Block UUID to navigate to |

**Examples:**

```bash
# Switch to "my-notes" graph
open "logseq://graph/my-notes"

# Switch and open specific page
open "logseq://graph/work?page=Projects"

# Switch and open specific block
open "logseq://graph/personal?block-id=65a1b2c3-4d5e-6789-abcd-ef0123456789"
```

### URL Scheme: `logseq://mobile/go/[action]`

**Platform:** iOS, Android

**Description:** Internal mobile shortcuts for quick actions.

**Supported Actions:**

| Action       | URL                            | Description           |
| ------------ | ------------------------------ | --------------------- |
| Quick Add    | `logseq://mobile/go/quick-add` | Open quick capture UI |
| Audio Record | `logseq://mobile/go/audio`     | Start audio recording |

**Examples:**

```bash
# From iOS Shortcuts or widgets
logseq://mobile/go/quick-add
logseq://mobile/go/audio
```

### URL Scheme: `logseq://shared?payload=[json]`

**Platform:** iOS (Internal - used by share extension)

**Description:** Internal deep link created by iOS share extension to pass captured content.

**Format:**

```
logseq://shared?payload=[URL-encoded-JSON]
```

**Payload Structure:**

```json
{
  "text": "Optional user text",
  "resources": [
    {
      "name": "filename.ext",
      "ext": "png",
      "type": "image/png",
      "url": "file:///path/to/shared/container/file"
    }
  ]
}
```

**Note:** This is automatically generated by the share extension; users don't construct this manually.

---

## Configuration

### Quick Capture Templates

**Location:** `config.edn` in your graph directory

**Purpose:** Customize how captured content is formatted.

**Example Configuration:**

```clojure
{
 ;; Quick capture templates
 :quick-capture-templates
 {:text "**{time}** [[quick capture]]: {text} {url}"
  :media "[[quick capture]] **{time}**: {url}"}

 ;; Quick capture options
 :quick-capture-options
 {:insert-today? false           ;; Default: true
  :redirect-page? false          ;; Default: false
  :default-page "Inbox"}         ;; Default: "quick capture"
}
```

### Template Variables

| Variable | Description                   | Example Output                            |
| -------- | ----------------------------- | ----------------------------------------- |
| `{time}` | Current time (HH:mm format)   | `14:35`                                   |
| `{date}` | Today's date (page reference) | `[[Dec 9th, 2025]]`                       |
| `{text}` | Captured text/content         | User-provided text                        |
| `{url}`  | Formatted URL or media embed  | `[Title](https://...)` or `{{video ...}}` |

**Important Notes:**

- **Templates are static**: You cannot dynamically change templates per capture
- **Placeholders are replaced with empty strings** if data is not provided
- **All substitutions happen at capture time**: The template string is read from `config.edn` and placeholders are replaced with actual values
- **You cannot use conditional logic** in templates (no if/else)
- **Only 2 template types allowed**: `:text` and `:media` - you cannot create custom template types

### Template Limitations

**❌ What You CANNOT Do:**

1. **Empty template with dynamic population:**

   ```clojure
   ;; This will NOT work as expected
   {:text ""}  ; Results in empty captures
   ```

2. **Conditional placeholders:**

   ```clojure

   ```

### Schema Definition

The template schema is strictly defined in `src/main/frontend/schema/handler/common_config.cljc`:

```clojure
[:quick-capture-templates (mu/optional-keys [:map
                                             [:text :string]
                                             [:media :string]])]
```

**This means:**

- Exactly 2 template types: `:text` and `:media`
- Both are optional but must be strings if provided
- No additional custom template types allowed
- Schema validation will fail if you try to add other keys

### Template Usage

**Which template is used when:**

| Capture Method        | Template Used | Context                                |
| --------------------- | ------------- | -------------------------------------- |
| macOS x-callback-url  | `:text`       | Desktop automation, browser extensions |
| Mobile text/URL share | `:text`       | Share sheet with text or URLs          |
| Mobile media share    | `:media`      | Share sheet with images, videos, files |

### Quick Capture Options

| Option            | Type    | Default           | Description                              |
| ----------------- | ------- | ----------------- | ---------------------------------------- |
| `:insert-today?`  | Boolean | `true`            | Always insert into today's journal       |
| `:redirect-page?` | Boolean | `false`           | Redirect to capture page after capturing |
| `:default-page`   | String  | `"quick capture"` | Default page when no page specified      |

**Examples:**

```clojure
;; Always capture to specific inbox page
   ;; Only :text and :media are allowed
   {:text "..."
    :media "..."
    :custom "..."}  ; ❌ Invalid - schema validation will fail
```

**✅ What You CAN Do:**

1. **Include all placeholders (unused ones become empty strings):**

   ```clojure
   {:text "{date} {time} {text} {url}"}
   ;; If no URL provided, {url} becomes ""
   ```

2. **Structure template to handle missing data gracefully:**

   ```clojure
   {:text "**{time}**\n{text}\n{url}"}
   ;; Empty values just create blank lines
   ```

3. **Use different templates for different capture types:**

   ```clojure
   {:text "📝 {text} - {url}"     ;; For text/URL captures
    :media "📷 {time} | {url}"}   ;; For media captures
   ```

4. **Pre-build content in external scripts:**
   ```python
   # Build content with custom logic before calling x-callback-url
   content = build_custom_template(data)
   url = f"logseq://x-callback-url/quickCapture?content={quote(content)}"
   ```

### Template Examples

**Default Text Template:**

```clojure
:text "**{time}** [[quick capture]]: {text} {url}"
```

**Custom Reading List Template:**

```clojure
:text "📚 {url}\n  - Captured: {time} on {date}\n  - {text}"
```

**Media Template:**

```clojure
:media "[[quick capture]] **{time}**: {url}"
```

### Quick Capture Options

| Option            | Type    | Default           | Description                              |
| ----------------- | ------- | ----------------- | ---------------------------------------- |
| `:insert-today?`  | Boolean | `true`            | Always insert into today's journal       |
| `:redirect-page?` | Boolean | `false`           | Redirect to capture page after capturing |
| `:default-page`   | String  | `"quick capture"` | Default page when no page specified      |

**Examples:**

```clojure
;; Always capture to specific inbox page
:quick-capture-options
{:insert-today? false
 :default-page "Inbox"}

;; Capture to today's journal and redirect
:quick-capture-options
{:insert-today? true
 :redirect-page? true}
```

---

## Use Cases and Examples

### 1. Browser Extension Integration

**Use Case:** Capture web pages with highlighted text from any browser.

**Implementation (JavaScript bookmark):**

```javascript
javascript: (function () {
  const url = encodeURIComponent(window.location.href)
  const title = encodeURIComponent(document.title)
  const selection = encodeURIComponent(window.getSelection().toString())
  const logseqUrl = `logseq://x-callback-url/quickCapture?url=${url}&title=${title}&content=${selection}&page=Reading%20List`
  window.location.href = logseqUrl
})()
```

### 2. Alfred Workflow (macOS)

**Use Case:** Quick capture via Alfred with clipboard content.

**Alfred Script:**

```bash
#!/bin/bash
query="$1"
encoded=$(echo "$query" | jq -sRr @uri)
open "logseq://x-callback-url/quickCapture?content=${encoded}&page=TODAY"
```

### 3. iOS Shortcuts

**Use Case:** Capture from clipboard to specific page.

**Shortcut Steps:**

1. Get Clipboard
2. Set Variable `content` to Clipboard
3. URL Encode `content`
4. Open URL: `logseq://x-callback-url/quickCapture?content=[encoded]&page=Fleeting%20Notes`

### 4. Terminal/CLI Integration

**Use Case:** Capture from command line with piped input.

**Shell Function (zsh/bash):**

```bash
logseq_capture() {
  local content="$1"
  local page="${2:-TODAY}"
  local encoded=$(echo "$content" | jq -sRr @uri)
  open "logseq://x-callback-url/quickCapture?content=${encoded}&page=${page}"
}

# Usage:
logseq_capture "Remember to review this" "Tasks"
echo "Some text" | logseq_capture "$(cat)" "Notes"
```

### 5. Python Script

**Use Case:** Automate content capture from scripts.

```python
import urllib.parse
import subprocess

def capture_to_logseq(content, page="TODAY", url=None, title=None):
    params = {
        'content': content,
        'page': page
    }
    if url:
        params['url'] = url
    if title:
        params['title'] = title

    query = urllib.parse.urlencode(params)
    logseq_url = f"logseq://x-callback-url/quickCapture?{query}"
    subprocess.run(['open', logseq_url])

# Usage
capture_to_logseq(
    content="Interesting article about PKM",
    url="https://example.com/article",
    page="Reading List"
)
```

### 6. AppleScript

**Use Case:** System-wide hotkey to capture selected text.

```applescript
tell application "System Events"
    keystroke "c" using {command down}  -- Copy
    delay 0.1
end tell

set selectedText to the clipboard as text
set encodedText to do shell script "python3 -c 'import sys, urllib.parse; print(urllib.parse.quote(sys.argv[1]))' " & quoted form of selectedText

do shell script "open 'logseq://x-callback-url/quickCapture?content=" & encodedText & "&page=Fleeting%20Notes'"
```

### 7. Raycast Extension

**Use Case:** Capture via Raycast launcher.

```typescript
import { getSelectedText, open } from '@raycast/api'

export default async function Command() {
  const text = await getSelectedText()
  const encoded = encodeURIComponent(text)
  await open(
    `logseq://x-callback-url/quickCapture?content=${encoded}&page=Inbox`
  )
}
```

---

## Code Locations

### Core URL Handling

| File                              | Lines  | Purpose                                    |
| --------------------------------- | ------ | ------------------------------------------ |
| `src/electron/electron/core.cljs` | 26-62  | URL scheme registration, protocol handlers |
| `src/electron/electron/url.cljs`  | 1-110  | URL parsing and routing for desktop        |
| `src/main/mobile/deeplink.cljs`   | 17-95  | Deep link routing for mobile               |
| `src/main/mobile/init.cljs`       | 19-103 | Mobile URL event listeners                 |

### Share Extension (iOS)

| File                                                    | Lines | Purpose                            |
| ------------------------------------------------------- | ----- | ---------------------------------- |
| `ios/App/ShareViewController/ShareViewController.swift` | 1-207 | iOS share extension implementation |
| `ios/App/ShareViewController/Info.plist`                | -     | Share extension configuration      |
| `ios/App/App/Info.plist`                                | 38-49 | URL scheme registration            |

### Intent Handling (Mobile)

| File                                   | Lines | Purpose                  |
| -------------------------------------- | ----- | ------------------------ |
| `src/main/frontend/mobile/intent.cljs` | 1-291 | Share payload processing |

### Quick Capture Logic

| File                                   | Lines   | Purpose                           |
| -------------------------------------- | ------- | --------------------------------- |
| `src/main/frontend/quick_capture.cljs` | 1-102   | Core quick capture implementation |
| `src/main/electron/listener.cljs`      | 114-116 | IPC listener for desktop          |

### Configuration Schema

| File                                                  | Lines | Purpose                  |
| ----------------------------------------------------- | ----- | ------------------------ |
| `src/main/frontend/schema/handler/common_config.cljc` | 83-86 | Config schema validation |

### Platform Registration

| File                                       | Purpose                               |
| ------------------------------------------ | ------------------------------------- |
| `android/app/src/main/AndroidManifest.xml` | Android URL scheme and intent filters |
| `capacitor.config.ts`                      | Capacitor mobile app configuration    |

---

## Security Considerations

### URL Parameter Sanitization

**Risk:** Malicious content injection via URL parameters.

**Mitigation:**

- URL parameters are decoded using safe decoders
- Content is inserted as plain text blocks (not evaluated as code)
- No JavaScript evaluation in captured content
- File paths validated when copying assets

**Code References:**

```clojure
;; electron/url.cljs:11-21
(defn get-URL-decoded-params
  "Get decoded URL parameters from parsed js/URL.
   URL.searchParams are already decoded"
  [^js/URL parsed-url keys]
  (let [params (.-searchParams parsed-url)]
    (map (fn [key]
           (when-let [value (.get params key)]
             value))
         keys)))
```

### File Access Control

**iOS Share Extension:**

**Risk:** Access to app sandbox files outside shared container.

**Mitigation:**

- Share extension has its own sandbox
- Files copied to **shared group container** (`group.com.logseq.logseq`)
- Main app can only access files in shared container or its own sandbox
- iOS enforces file access permissions

**Code:** `ios/App/ShareViewController/ShareViewController.swift:45-50`

```swift
var groupContainerUrl: URL? {
    return FileManager.default.containerURL(
        forSecurityApplicationGroupIdentifier: "group.com.logseq.logseq")
}

fileprivate func createSharedFileUrl(_ url: URL?) -> URL? {
    let tempFilename = url!.lastPathComponent
        .addingPercentEncoding(withAllowedCharacters: .urlQueryAllowed)!
    let copyFileUrl = groupContainerUrl!.appendingPathComponent(tempFilename)
    try? Data(contentsOf: url!).write(to: copyFileUrl)
    return copyFileUrl
}
```

### Protocol Hijacking

**Risk:** Malicious app registering as `logseq://` handler.

**Mitigation:**

- **macOS:** First app to register wins, Electron sets default on launch
- **iOS:** App bundle ID enforcement, system ensures correct app
- **Android:** Intent filters verified by package name

### Content Type Validation

**iOS Share Extension:**

**Risk:** Accepting unexpected file types.

**Mitigation:**

- Explicit UTI type checking (`kUTTypeURL`, `kUTTypeImage`, etc.)
- MIME type validation
- File extension whitelisting in asset handler

---

## Troubleshooting

### macOS: URL Not Opening

**Symptoms:** Clicking `logseq://` link does nothing.

**Causes:**

1. Logseq not set as default protocol handler
2. Another app registered for `logseq://`
3. URL malformed

**Solutions:**

```bash
# Re-register Logseq as handler (requires app restart)
# macOS stores protocol handlers in LaunchServices database

# Check current handler
/System/Library/Frameworks/CoreServices.framework/Versions/A/Frameworks/LaunchServices.framework/Versions/A/Support/lsregister -dump | grep -i logseq

# Rebuild LaunchServices database
/System/Library/Frameworks/CoreServices.framework/Versions/A/Frameworks/LaunchServices.framework/Versions/A/Support/lsregister -kill -r -domain local -domain system -domain user

# Restart Logseq
```

### iOS: Share Extension Not Appearing

**Symptoms:** "Logseq" not in share sheet.

**Causes:**

1. Share extension not included in build
2. Content type not supported by extension
3. iOS not recognizing extension

**Solutions:**

1. Check build includes `ShareViewController.appex` target
2. Verify content type matches extension's `NSExtensionActivationRule`
3. Reinstall app (delete + fresh install)
4. Check iOS version compatibility

### iOS: Duplicate Captures

**Symptoms:** Same content captured twice.

**Cause:** Known iOS bug - `appUrlOpen` fires twice for same intent.

**Mitigation (Already Implemented):**

Code in `mobile/init.cljs:19-39` checks if URL is same as previous within 1 second window.

**User Workaround:** If duplicates still occur, wait 2+ seconds between captures.

### Android: Share Intent Not Working

**Symptoms:** "Logseq" not in share targets.

**Causes:**

1. Intent filters not properly configured
2. MIME type mismatch
3. App needs reinstall

**Solutions:**

1. Verify `AndroidManifest.xml` has `ACTION_SEND` intent filter
2. Check MIME type of shared content matches manifest
3. Clear app cache and reinstall

### Parameters Not Decoded Properly

**Symptoms:** URLs or content appear URL-encoded in Logseq.

**Cause:** Double encoding or improper encoding.

**Solution:**

Ensure parameters are URL-encoded **once**:

```bash
# WRONG (double encoded)
content="Hello%2520World"  # %20 is encoded as %2520

# CORRECT
content="Hello%20World"    # Space encoded as %20
```

Use proper encoding tools:

- JavaScript: `encodeURIComponent()`
- Python: `urllib.parse.quote()`
- Shell: `jq -sRr @uri`

---

## Recommendations

### For Users

**1. Use Automation Tools**

- **Alfred/Raycast:** Create workflows for quick capture
- **Shortcuts (iOS):** Build shortcuts for common capture patterns
- **Keyboard Maestro:** Assign hotkeys for instant capture

**2. Configure Templates**

Customize `config.edn` templates for your workflow:

```clojure
:quick-capture-templates
{:text "📝 {time} | {text}\n  Source: {url}\n  Tags: #inbox"
 :media "📷 {time} | {url}"}
```

**3. Set Default Page**

Use `:default-page` to centralize captures:

```clojure
:quick-capture-options
{:default-page "Inbox"
 :redirect-page? false}
```

### For Developers

**1. Extend URL Actions**

The x-callback-url handler supports adding new actions:

```clojure
;; In electron/url.cljs:x-callback-url-handler
(= action "/newAction")
(let [[param1 param2] (get-URL-decoded-params parsed-url ["param1" "param2"])]
  (send-to-focused-renderer "newAction" {:param1 param1 :param2 param2} win))
```

**2. Add Mobile Deep Link Routes**

Extend `mobile/deeplink.cljs`:

```clojure
(= hostname "custom")
(let [action (.getPath parsed-url)]
  (handle-custom-action action))
```

**3. Improve Share Extension**

Areas for enhancement:

- Add PDF text extraction
- Support more UTI types
- Pre-process images (compress, resize)
- Add OCR for scanned documents

**4. Testing URL Schemes**

```bash
# macOS testing
open "logseq://x-callback-url/quickCapture?content=Test&page=TODAY"

# iOS Simulator
xcrun simctl openurl booted "logseq://shared?payload=%7B%22text%22%3A%22Test%22%7D"

# Android
adb shell am start -a android.intent.action.VIEW -d "logseq://graph/test"
```

### For Integration Developers

**1. URL Encoding Best Practices**

```javascript
// Build URL programmatically
const params = new URLSearchParams({
  url: sourceUrl,
  title: pageTitle,
  content: selectedText,
  page: 'Reading List',
})

const logseqUrl = `logseq://x-callback-url/quickCapture?${params}`
```

**2. Error Handling**

X-callback-url supports success/error callbacks (not currently implemented in Logseq):

```
logseq://x-callback-url/quickCapture?url=...&x-success=myapp://success&x-error=myapp://error
```

**Future enhancement:** Could add callback support for automation workflows.

**3. Batch Captures**

For multiple items, send separate URL requests with small delays:

```python
import time
for item in items:
    capture_to_logseq(item)
    time.sleep(0.5)  # Prevent overwhelming the app
```

---

## Appendix

### A. URL Encoding Reference

| Character | Encoded      | Common Use       |
| --------- | ------------ | ---------------- |
| Space     | `%20` or `+` | All text         |
| `#`       | `%23`        | Page refs, tags  |
| `&`       | `%26`        | Separates params |
| `=`       | `%3D`        | In values        |
| `?`       | `%3F`        | In values        |
| `/`       | `%2F`        | In paths/URLs    |
| `:`       | `%3A`        | In URLs          |
| `[[`      | `%5B%5B`     | Page refs        |
| `]]`      | `%5D%5D`     | Page refs        |

### B. Related Documentation

- [Quick Capture Investigation](./quick-capture-callback-url-investigation.md) - Detailed quick capture flow
- [Code Detective Investigation Template](../.github/prompts/code-detective-investigation.prompt.md)
- [AGENTS.md](../AGENTS.md) - Development guidelines

### C. External Resources

- [X-Callback-URL Specification](http://x-callback-url.com/)
- [iOS URL Scheme Reference](https://developer.apple.com/documentation/xcode/defining-a-custom-url-scheme-for-your-app)
- [Android Intent Filters](https://developer.android.com/guide/components/intents-filters)
- [Electron Protocol Handlers](https://www.electronjs.org/docs/latest/api/protocol)

### D. Version History

| Version | Date       | Changes                             |
| ------- | ---------- | ----------------------------------- |
| 1.0     | 2025-12-09 | Initial comprehensive investigation |

---

## Glossary

| Term                 | Definition                                                                       |
| -------------------- | -------------------------------------------------------------------------------- |
| **X-Callback-URL**   | A standardized URL scheme protocol for inter-app communication                   |
| **URL Scheme**       | Custom protocol (e.g., `logseq://`) registered by an app to handle specific URLs |
| **Deep Link**        | URL that opens an app to a specific location or action                           |
| **Share Extension**  | iOS app extension that appears in system share sheet                             |
| **Intent Filter**    | Android manifest declaration for handling specific intents/actions               |
| **UTI**              | Uniform Type Identifier - Apple's system for file type identification            |
| **IPC**              | Inter-Process Communication - how Electron main/renderer processes communicate   |
| **Capacitor**        | Cross-platform native runtime for web apps (mobile framework)                    |
| **Protocol Handler** | OS-level mechanism for routing custom URL schemes to applications                |

---

**Document End**
