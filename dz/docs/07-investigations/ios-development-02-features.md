# Logseq iOS Development Investigation — Part 2

## Features in Scope & Compatibility Matrix

**Investigation Date:** February 19, 2026  
**Last Verified:** May 29, 2026  
**Status:** Development planning baseline

> **Last revised:** 2026-05-29

---

## Executive Summary

iOS has strong coverage for core note-taking, graph editing, and database graph workflows, but there are hard platform/runtime constraints compared to desktop Electron.

Most planning errors come from assuming desktop plugin/PDF/git capabilities carry over to iOS. They do not.

## Findings: New Capabilities

- Native iOS graph actions are routed through the LiquidTabs plugin path, including graph list updates, tab readiness, and open/download/delete/leave actions.
- Flashcards are wired as a first-class mobile tab with FSRS-backed UI and mobile-specific header/selector state.

---

## 1) Features In Scope (iOS)

The following are in active scope for iOS development and support:

- Core block/page editing workflows
- Journals, page links, block references, templates
- Database graph support (SQLite-backed graph mode)
- File-based graph support (with limitations)
- Query execution (DSL and advanced query paths where supported by UI/workflow)
- Real-time sync/collaboration integration (RTC path)
- Flashcards (native mobile tab, FSRS-backed)

---

## 2) Out of Scope / Not Supported on iOS

### Hard unsupported

- Plugin runtime (desktop Electron plugin architecture not available on iOS WebView runtime)
- Git integration for file-based workflows on mobile
- Desktop HTTP API server behavior
- MCP tooling and desktop devtool-only integration paths
- Whiteboards as a standalone mobile feature claim; current mobile source has no dedicated whiteboard surface or iOS-specific whiteboard flow.
- Theme usage/custom CSS via the desktop plugin/theme path; current theme customisation is gated behind `config/lsp-enabled?`, which is false on mobile.

### Known major platform gap

- PDF annotation/viewer feature parity is not available on iOS (see maintained platform-differences reference)

---

## 3) Limited or Conditional Support

- Local media behavior has mobile-specific constraints
- Keyboard-shortcut-heavy workflows depend on external keyboard
- External links and file handoff behavior defer to iOS system integration patterns
- Large graph performance depends strongly on device memory class

---

## 4) Compatibility Matrix

### Runtime / Platform Matrix

| Dimension           | iOS Status               | Notes                             |
| ------------------- | ------------------------ | --------------------------------- |
| Runtime shell       | Capacitor + WebView      | Not Electron                      |
| Plugin architecture | ❌ Not supported         | Desktop plugin model unavailable  |
| Database graphs     | ✅ Supported             | Primary modern path               |
| File-based graphs   | ⚠️ Supported with limits | No git sync on mobile             |
| RTC Sync            | ✅ Supported             | With mobile lifecycle constraints |
| Flashcards          | ✅ Supported             | Native mobile tab + FSRS          |
| PDF annotations     | ❌ Not supported         | Known issue on iOS                |

### Version / Dependency Matrix (Current Repo Baseline)

| Component                               | Value / Range                         | Source                                  |
| --------------------------------------- | ------------------------------------- | --------------------------------------- |
| iOS platform target (Podfile baseline)  | iOS 15.0                              | `ios/App/Podfile`                       |
| iOS deployment targets in Xcode project | 15.0 / 16.6 / 26.0 (target-dependent) | `ios/App/App.xcodeproj/project.pbxproj` |
| Capacitor core packages                 | 8.3.1 family                          | `package.json`                          |
| CocoaPods                               | required                              | `docs/develop-logseq-on-mobile.md`      |

> Note: device-model coverage is tracked in the new appendix: `ios-development-04-device-compatibility-appendix.md`.

---

## 5) Resource Constraints & Planning Guidance

- Mobile memory envelope is lower than desktop; large-graph behavior must be tested on real devices.
- Assume tighter CPU and background execution constraints than desktop.
- Prioritize incremental rendering and avoid desktop-size assumptions for heavy graph operations.

---

## 6) Platform-Gating in Code

Mobile/ios checks are centralized in platform helper code and referenced by frontend features:

- Platform helpers: `src/main/frontend/mobile/util.cljs`
- Global UI class tagging and mobile-specific behavior branches live in frontend UI/components

When implementing new features, add/verify iOS-specific paths explicitly instead of relying on desktop defaults.

---

## 7) Delivery Checklist for iOS Feature Work

- [ ] Feature marked as in-scope for iOS runtime
- [ ] Platform-gated behavior implemented (`native-ios?` and related helpers)
- [ ] iPhone + iPad behavior validated
- [ ] Memory/perf tested on at least one lower-memory device class
- [ ] Sync implications reviewed if feature changes graph mutations

---

## 8) References

- Platform differences: [ddocs/reference/platforms/mobile-differences.md](../ddocs/reference/platforms/mobile-differences.md)
- Mobile helpers: [src/main/frontend/mobile/util.cljs](../../../src/main/frontend/mobile/util.cljs)
- Mobile onboarding: [docs/develop-logseq-on-mobile.md](../../../docs/develop-logseq-on-mobile.md)
- Investigation index: [ios-development-00-index.md](./ios-development-00-index.md)
- Device matrix appendix: [ios-development-04-device-compatibility-appendix.md](./ios-development-04-device-compatibility-appendix.md)
