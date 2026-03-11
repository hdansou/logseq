---
title: 'Installation Guide'
status: 🟢
since-version: 'N/A'
quadrant: 'Tutorial'
last-verified: '2026-03-10'
platform: 'All'
related-docs:
  - 'tutorials/getting-started/02-first-graph.md'
---

# Installation Guide

> **Last revised:** 2026-03-10

Welcome to Logseq! This tutorial will guide you through installing Logseq on your device in under 10 minutes.

## What is Logseq?

Logseq is a local-first, privacy-focused knowledge management and note-taking application. It uses a block-based structure with bidirectional linking to help you build a personal knowledge graph.

## Choose Your Platform

### Desktop (Windows, macOS, Linux)

#### Option 1: Download from Website (Recommended)

1. Visit [https://logseq.com/downloads](https://logseq.com/downloads)
2. Click the download button for your operating system
3. Wait for the installer to download

**Installation Steps:**

**macOS:**

- Open the downloaded `.dmg` file
- Drag Logseq to your Applications folder
- Open Logseq from Applications (you may need to approve it in System Preferences → Security & Privacy)

**Windows:**

- Run the downloaded `.exe` installer
- Follow the installation wizard
- Launch Logseq from the Start Menu or Desktop shortcut

**Linux:**

- Download the `.AppImage` file
- Make it executable: `chmod +x Logseq-linux-x64-*.AppImage`
- Run it: `./Logseq-linux-x64-*.AppImage`
- For system-wide installation, consider using your distribution's package manager if available

#### Option 2: Package Managers

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

1. Open the **App Store**
2. Search for **"Logseq"**
3. Tap **Get** → **Install**
4. Open Logseq from your home screen

**Requirements:** iOS 14.0 or later

#### Android

1. Open the **Google Play Store**
2. Search for **"Logseq"**
3. Tap **Install**
4. Open Logseq from your app drawer

**Requirements:** Android 6.0 (Marshmallow) or later

### Web Version

Logseq also offers a web version for quick access without installation:

1. Visit [https://logseq.com/app](https://logseq.com/app)
2. Your data stays in your browser's local storage
3. **Note:** Limited features compared to desktop; best for quick access

## First Launch

When you first open Logseq, you'll see a welcome screen with setup options:

1. **Choose Your Graph Storage:**

   - **Local Folder** (File-based): Store notes as markdown files in a folder
   - **Database Mode** (Recommended for new users): Store notes in a local database

2. **Select Language:** Choose your preferred interface language

3. **Quick Tour:** Optionally take a guided tour of Logseq's interface

## System Requirements

### Desktop

- **OS:** Windows 10+, macOS 10.15+ (Catalina or later), or modern Linux distribution
- **RAM:** 4GB minimum, 8GB recommended
- **Storage:** 500MB for app + storage for your graphs
- **Display:** 1280x720 minimum resolution

### Mobile

- **iOS:** 14.0 or later
- **Android:** 6.0 (Marshmallow) or later
- **Storage:** 200MB for app + storage for your graphs

## Troubleshooting

### macOS: "App can't be opened because it is from an unidentified developer"

1. Go to **System Preferences → Security & Privacy**
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
- [Community Discord](https://discord.gg/logseq)
- [Community Forum](https://discuss.logseq.com)
