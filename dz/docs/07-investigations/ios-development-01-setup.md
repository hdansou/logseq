# Logseq iOS Development Investigation — Part 1

## Setup & Build Pipeline for New Developers

**Investigation Date:** February 19, 2026

> **Last revised:** 2026-05-29  
> **Status:** Actionable onboarding baseline

---

## Executive Summary

The quickest stable path to iOS development is:

1. Prepare toolchain (Xcode, CocoaPods, Node, Capacitor dependencies)
2. Run web/mobile watch process
3. Sync Capacitor assets and plugins
4. Build/run from Xcode or `bb dev:ios-app`

The major failure points are usually CocoaPods/ffi on Apple Silicon, stale Capacitor sync state, and local server misconfiguration in `capacitor.config.ts`.

## Findings: New Capabilities

- `bb dev:mobile-https-cert` is now a standalone helper for generating the HTTPS trust chain used by mobile development.
- `bb dev:ios-app` remains the one-command iOS dev path, and it now wraps certificate generation, `pnpm mobile-watch`, `pnpm exec cap sync ios`, and `pnpm exec cap open ios`.
- `bb release:ios-app` remains available and delegates to `pnpm run-ios-release` for the release install flow.

---

## 1) Environment Prerequisites

From `docs/develop-logseq-on-mobile.md` and project config:

- Xcode 26.x installed; current CI baseline uses Xcode 26.0.1
- CocoaPods installed (`sudo gem install cocoapods`)
- Node compatible with project (`package.json` engines)
- Capacitor iOS dependencies present (`@capacitor/ios` and peers)
- Optional helper tooling for one-command workflows:
  - `babashka` for `bb` tasks
  - `gnu-sed` (`gsed`) on macOS for mobile bb tasks

### Apple Silicon (M1/M2/M3) CocoaPods fix

Run from `ios/App` if ffi-related pod install failures occur:

```sh
arch -x86_64 sudo gem install ffi
arch -x86_64 pod install
```

---

## 2) Development Build Loop

### Standard manual flow

From repository root:

```sh
pnpm install && pnpm mobile-watch
```

In another terminal:

```sh
pnpm exec cap sync ios
pnpm exec cap open ios
```

Then build/run from Xcode on simulator or device.

### One-command flow

```sh
bb dev:ios-app
```

This wraps the development setup steps when the machine has required dependencies, including the HTTPS cert helper and the iOS Capacitor sync/open steps.

---

## 3) Release Build Loop

Before release build, ensure the development server override is disabled in `capacitor.config.ts`.

Release path:

```sh
pnpm run-ios-release
```

This is wrapped by `bb release:ios-app`, which calls the release task used across the repository.

---

## 4) Key Files You Will Touch

- `capacitor.config.ts`
  - Controls Capacitor behavior and optional development `server` override.
- `ios/App/Podfile`
  - CocoaPods dependency resolution and iOS platform target.
- `ios/App/App/Info.plist`
  - Native permissions and app capabilities.
- `package.json`
  - Scripts and Capacitor package versions.

---

## 5) iOS Native Surface Area in This Repository

Native iOS project files under `ios/App/App/` include:

- App lifecycle and bridge: `AppDelegate.swift`, `SceneDelegate.swift`
- Native UX components: bottom sheet/top bar/editor toolbar plugins
- File and folder picker integrations
- Sync-related crypto utility files (for mobile sync plumbing)

This means iOS work is often hybrid: ClojureScript UI logic + native bridge/plugin behavior.

---

## 6) Permissions & Capabilities Snapshot

`Info.plist` defines iOS-native permissions/capabilities used by mobile features, including:

- Camera
- Microphone
- Photo library
- Speech recognition
- File/document access and sharing behaviors

When adding iOS-native features, validate that permission prompts and usage descriptions are updated consistently.

---

## 7) Common Build/Run Failures

### Symptom: code changes do not appear in app

Likely stale sync/build state. Restart watch process and resync:

```sh
# terminal 1
pnpm mobile-watch

# terminal 2
pnpm exec cap sync ios
```

### Symptom: pod install fails with ffi errors on Apple Silicon

Use the `arch -x86_64` ffi/pod workaround documented above.

### Symptom: app cannot reach local web server

Verify development server settings in `capacitor.config.ts` and that local IP/port are reachable from device/simulator.

---

## 8) New Developer Checklist

- [ ] Can run `yarn mobile-watch` successfully
- [ ] Can run `pnpm mobile-watch` successfully
- [ ] Can run `pnpm exec cap sync ios` without plugin/pod errors
- [ ] Can launch and build iOS project from Xcode
- [ ] Can run bb-based shortcut commands, including `bb dev:mobile-https-cert`, `bb dev:ios-app`, and `bb release:ios-app` (optional but recommended)
- [ ] Understands where platform-specific code lives (`src/main/frontend/mobile/` and `ios/App/App/`)

---

## 9) References

- Main mobile dev guide: [docs/develop-logseq-on-mobile.md](../../../docs/develop-logseq-on-mobile.md)
- iOS project root: [ios/App/](../../../ios/App/)
- Shared frontend mobile helpers: [src/main/frontend/mobile/](../../../src/main/frontend/mobile/)
- Investigation index: [ios-development-00-index.md](./ios-development-00-index.md)
