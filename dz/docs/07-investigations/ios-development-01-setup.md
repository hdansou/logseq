# Logseq iOS Development Investigation — Part 1

## Setup & Build Pipeline for New Developers

**Investigation Date:** February 19, 2026  
**Status:** Actionable onboarding baseline

---

## Executive Summary

The quickest stable path to iOS development is:

1. Prepare toolchain (Xcode, CocoaPods, Node, Capacitor dependencies)
2. Run web/mobile watch process
3. Sync Capacitor assets and plugins
4. Build/run from Xcode or `bb dev:ios-app`

The major failure points are usually CocoaPods/ffi on Apple Silicon, stale Capacitor sync state, and local server misconfiguration in `capacitor.config.ts`.

---

## 1) Environment Prerequisites

From `docs/develop-logseq-on-mobile.md` and project config:

- Xcode 13+ installed
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
yarn && yarn mobile-watch
```

In another terminal:

```sh
npx cap sync ios
npx cap open ios
```

Then build/run from Xcode on simulator or device.

### One-command flow

```sh
bb dev:ios-app
```

This wraps the development setup steps when the machine has required dependencies.

---

## 3) Release Build Loop

Before release build, ensure the development server override is disabled in `capacitor.config.ts`.

Release path:

```sh
yarn run-ios-release
```

or

```sh
bb release:ios-app
```

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
yarn mobile-watch

# terminal 2
npx cap sync ios
```

### Symptom: pod install fails with ffi errors on Apple Silicon

Use the `arch -x86_64` ffi/pod workaround documented above.

### Symptom: app cannot reach local web server

Verify development server settings in `capacitor.config.ts` and that local IP/port are reachable from device/simulator.

---

## 8) New Developer Checklist

- [ ] Can run `yarn mobile-watch` successfully
- [ ] Can run `npx cap sync ios` without plugin/pod errors
- [ ] Can launch and build iOS project from Xcode
- [ ] Can run bb-based shortcut commands (optional but recommended)
- [ ] Understands where platform-specific code lives (`src/main/frontend/mobile/` and `ios/App/App/`)

---

## 9) References

- Main mobile dev guide: `docs/develop-logseq-on-mobile.md`
- iOS project root: `ios/App/`
- Shared frontend mobile helpers: `src/main/frontend/mobile/`
- Investigation index: `ios-development-00-index.md`
