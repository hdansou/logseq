---
title: 'Installation Guide'
status: 🟢
since-version: 'N/A'
quadrant: 'Tutorial'
last-verified: '2026-09-11'
verified-against: 'be800f1711'
review-status: audited
platform: 'All'
related-docs:
  - 'tutorials/getting-started/02-first-graph.md'
---

# Installation Guide

> **Last revised:** 2026-09-11

Welcome to Logseq! This tutorial will guide you through installing Logseq on your device in under 10 minutes.

## What is Logseq?

Logseq is a local-first, privacy-focused knowledge management and note-taking application. It uses a block-based structure with bidirectional linking to help you build a personal knowledge graph.

## Choose Your Platform

### Desktop (Windows, macOS, Linux)

#### Option 1: Download a Release Build

Desktop builds are published to **GitHub Releases** for the `logseq/logseq` repository ([`resources/electron-builder.yml:44-46`](../../../../../resources/electron-builder.yml)). The release workflow currently publishes the **beta** channel there; the `stable` option is commented out ([`.github/workflows/build-desktop-release.yml:13-14`](../../../../../.github/workflows/build-desktop-release.yml)).

1. Visit [https://github.com/logseq/logseq/releases](https://github.com/logseq/logseq/releases)
2. Download the file for your operating system and CPU (`x64` or `arm64` builds exist for all three platforms)
3. Wait for the download to finish

> **Unverified:** A download page at `https://logseq.com/downloads`. No build or workflow file publishes there.

**Installation Steps:**

**macOS:**

- Open the downloaded `.dmg` file (a `.zip` is also built) ([`resources/electron-builder.yml:55-60`](../../../../../resources/electron-builder.yml))
- Drag Logseq to your Applications folder
- Open Logseq from Applications

**Windows:**

- Run the downloaded installer, `Logseq-win-<arch>-<version>-nsis.exe` (a `.zip` is also built)
- The installer is not one-click: it walks you through the steps and lets you choose the install folder (`oneClick: false`, `allowToChangeInstallationDirectory: true`, [`resources/electron-builder.yml:74-77`](../../../../../resources/electron-builder.yml))
- Launch Logseq from the Start Menu or Desktop shortcut

**Linux:**

- Download the `.AppImage` file (a `.zip` is also built) ([`resources/electron-builder.yml:81-87`](../../../../../resources/electron-builder.yml))
- Make it executable: `chmod +x Logseq-linux-*.AppImage`
- Run it: `./Logseq-linux-x64-*.AppImage` (on ARM: `./Logseq-linux-arm64-*.AppImage`, [`.github/workflows/build-desktop-release.yml:324`](../../../../../.github/workflows/build-desktop-release.yml))

#### Option 2: Package Managers

The release workflow has an optional job that publishes to the **Snap Store** (`publish-linux-snap`, [`.github/workflows/build-desktop-release.yml:880-927`](../../../../../.github/workflows/build-desktop-release.yml)). It runs only when a release is dispatched with `publish-linux-stores` set.

> **Unverified:** The commands below. No build or workflow file publishes to Homebrew, Chocolatey, Scoop or Flathub, so these packages, if they exist, are maintained outside this repository.

**macOS (Homebrew):**

```bash
brew install --cask logseq
```

**Windows (Chocolatey):**

```bash
choco install logseq
```

**Windows (Scoop):**

```bash
scoop bucket add extras
scoop install logseq
```

**Linux (Flatpak):**

```bash
flatpak install flathub com.logseq.Logseq
```

### Mobile (iOS & Android)

#### iOS (iPhone & iPad)

The iOS release workflow builds the app and uploads it to **TestFlight** ([`.github/workflows/build-ios-release.yml:1`](../../../../../.github/workflows/build-ios-release.yml)).

> **Unverified:** An App Store listing. Nothing in the repository shows App Store distribution.

**Requirements:** iOS 16.6 or later. The `Logseq` app target sets `IPHONEOS_DEPLOYMENT_TARGET = 16.6` in both its Debug and Release configurations ([`ios/App/App.xcodeproj/project.pbxproj:674`](../../../../../ios/App/App.xcodeproj/project.pbxproj) and `:707`). The `platform :ios, '15.0'` in [`ios/App/Podfile:3`](../../../../../ios/App/Podfile) applies to the CocoaPods dependencies, not the app.

#### Android

The Android workflow builds a signed APK named `Logseq-android-<version>.apk` ([`.github/workflows/build-android.yml:157-160`](../../../../../.github/workflows/build-android.yml)). The desktop release job can attach that APK to the GitHub release ([`.github/workflows/build-desktop-release.yml:790`](../../../../../.github/workflows/build-desktop-release.yml)).

> **Unverified:** A Google Play Store listing. Nothing in the repository shows Play Store distribution.

**Requirements:** Android 7.0 (API 24) or later (`minSdkVersion = 24`, [`android/variables.gradle:2`](../../../../../android/variables.gradle)).

### Web Version

Logseq also offers a web version for quick access without installation:

1. Visit [https://app.logseq.com/](https://app.logseq.com/) ([`README.md:76`](../../../../../README.md))
2. Your graph is stored in the browser's Origin Private File System (OPFS), not in local storage. Logseq warns that the browser may clear OPFS storage when disk space runs low ([`src/main/frontend/persist_db/browser.cljs:119`](../../../../../src/main/frontend/persist_db/browser.cljs))

> **Unverified:** "Limited features compared to desktop." No source defines which features differ.

## First Launch

When you first open Logseq there is no setup screen and nothing to choose:

1. **A graph is created for you.** If no graphs exist yet, Logseq creates a database graph named `Demo` and opens it ([`src/main/frontend/handler.cljs:189-190`](../../../../../src/main/frontend/handler.cljs); name from [`src/main/frontend/config.cljs:288`](../../../../../src/main/frontend/config.cljs)). It also creates today's journal page and opens the Journals view ([`src/main/frontend/handler/repo.cljs:169-176`](../../../../../src/main/frontend/handler/repo.cljs)). The `Demo` graph does not appear in the **All graphs** list ([`src/main/frontend/components/repo.cljs:319`](../../../../../src/main/frontend/components/repo.cljs)).
2. **All graphs are database graphs.** You can't create or open a folder of Markdown files. The graph list only shows DB graphs ([`src/main/frontend/components/repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)), and existing Markdown notes come in through **Import** (next tutorial).
3. **Language:** First launch doesn't ask for a language. You can change it later under **Settings** → **Language** ([`src/main/frontend/components/settings.cljs:344`](../../../../../src/main/frontend/components/settings.cljs), [`:878`](../../../../../src/main/frontend/components/settings.cljs)).

> **Removed:** The "Choose Your Graph Storage" (Local Folder vs Database Mode) and "Quick Tour" first-launch steps. Startup creates the `Demo` DB graph without prompting, and neither the components nor `src/resources/dicts/en.edn` contain a tour.

## System Requirements

### Desktop

> **Unverified:** OS versions, RAM, disk space and display resolution. The desktop build config ([`resources/electron-builder.yml`](../../../../../resources/electron-builder.yml)) declares no minimum OS, memory, storage or display requirement.

### Mobile

- **iOS:** 16.6 or later ([`ios/App/App.xcodeproj/project.pbxproj:674`](../../../../../ios/App/App.xcodeproj/project.pbxproj))
- **Android:** 7.0 (API 24) or later ([`android/variables.gradle:2`](../../../../../android/variables.gradle))

> **Unverified:** Storage needed for the app. No source declares it.

## Findings: New Capabilities

- Mobile minimums: the iOS app target requires iOS 16.6 ([`ios/App/App.xcodeproj/project.pbxproj:674`](../../../../../ios/App/App.xcodeproj/project.pbxproj)), and Android targets API 24 ([`android/variables.gradle:2`](../../../../../android/variables.gradle)).
- The mobile apps use Capacitor 8 (`"@capacitor/core": "^8.3.1"`, [`package.json:130`](../../../../../package.json)).
- Desktop builds exist for both `x64` and `arm64` on macOS, Windows and Linux ([`.github/workflows/build-desktop-release.yml:669`](../../../../../.github/workflows/build-desktop-release.yml) lists the six build jobs).

## Troubleshooting

> **Unverified:** The steps in this section. They describe operating-system dialogs and packages, not Logseq code, so the Logseq source can't confirm them.

### macOS: "App can't be opened because it is from an unidentified developer"

1. Go to **System Settings → Privacy & Security**
2. Click **Open Anyway** next to the Logseq warning
3. Confirm by clicking **Open**

### Windows: SmartScreen Warning

1. Click **More info** on the SmartScreen popup
2. Click **Run anyway**

### Linux: AppImage Won't Run

1. Ensure the file is executable: `chmod +x Logseq-*.AppImage`
2. Install FUSE if needed: `sudo apt install fuse` (Ubuntu/Debian)

### Mobile: App Crashes on Launch

1. Ensure you have the latest OS version
2. Clear app cache (Settings → Apps → Logseq → Clear Cache)
3. Reinstall the app if issues persist

## Next Steps

Now that Logseq is installed, let's create your first graph:

👉 **Next:** [Your First Graph](02-first-graph.md)

## Additional Resources

- [Logseq Official Website](https://logseq.com)
- [Logseq GitHub Releases](https://github.com/logseq/logseq/releases)
- [Community Discord](https://discord.gg/KpN4eHY) (invite link from [`README.md:32`](../../../../../README.md))
- [Community Forum](https://discuss.logseq.com) ([`src/main/frontend/components/container.cljs:254`](../../../../../src/main/frontend/components/container.cljs))
