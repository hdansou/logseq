# Logseq Journal: Build and Test

> **Last revised:** 2026-09-27 · verified at logseq_journal `d4fbe23`, lui `df9fddb`

Logseq Journal is a native journal app with its own OCaml DB worker, sync client and storage. It draws through lui with SwiftUI on iOS and macOS. The repo has no README and no CI, so this guide pieces the build together from its scripts and its agent skills in `.agents/skills/`. Read the [overview](README.md) first.

## Contents

1. [Set up the OCaml environment](#1-set-up-the-ocaml-environment)
2. [Run the core tests](#2-run-the-core-tests)
3. [Build the iOS simulator app](#3-build-the-ios-simulator-app)
4. [Sign in and open a graph](#4-sign-in-and-open-a-graph)
5. [Build the macOS app](#5-build-the-macos-app)
6. [Diagnose problems](#6-diagnose-problems)
7. [Known issues](#known-issues)

## 1. Set up the OCaml environment

The journal needs its own opam switch, because its pins clash with Chat's. The Swift package also expects a lui checkout next to the repo, at the commit the opam file pins.

```sh
cd <parent folder>
git clone https://github.com/logseq/logseq_journal.git
git clone https://github.com/logseq/lui.git
git -C lui checkout <lui commit from logseq_journal.opam>   # df9fddb at the time of writing

cd logseq_journal
opam switch create journal-5.5.0 --packages=ocaml-base-compiler.5.5.0 -y
eval $(opam env --switch=journal-5.5.0)
opam install . --deps-only --locked --yes --with-test
opam install -y eio_posix.1.2 eio_main.1.2   # used by the code, not declared in any .opam file
```

- The Swift package reads lui from `../lui/platform/apple`. Set `JOURNAL_LUI_PACKAGE_PATH` if your checkout is elsewhere.
- Without the two `eio` packages, the build stops with `Library "eio_posix" not found`.

## 2. Run the core tests

```sh
eval $(opam env --switch=journal-5.5.0)
dune build @runtest
```

Expected result: 21 of 22 suites pass. `source_boundary_test` fails at the time of writing. It checks the source for a `V.progress` call in `app/journal_timeline.ml`, which commit `316b159` removed on 2026-09-24.

The repo also has Python probes in `tool/test_*.py` that drive the SwiftUI host through `tool/lui_probe_host.py`. They weren't tried for this guide.

## 3. Build the iOS simulator app

This is the path to use for sign-in and sync. The simulator reads the app's keychain entitlements from a section embedded at link time, so login works without an Apple certificate.

There is no cross-compiler step. The OCaml core is built for macOS, then its build-platform label is changed to the iOS simulator with Apple's `vtool`. Both run the same arm64 code.

**Official route, from the repo's agent notes:**

```sh
dune build @ios-app
```

**Manual route, verified for this guide:**

```sh
eval $(opam env --switch=journal-5.5.0)
dune build app/native_embed.exe.o
mkdir -p _build/apple/ios-sim-object
vtool -set-build-version 7 26.0 "$(xcrun --sdk iphonesimulator --show-sdk-version)" -replace \
  -output _build/apple/ios-sim-object/native_embed_sim.o \
  _build/default/app/native_embed.exe.o
JOURNAL_OCAML_OBJECT=$PWD/_build/apple/ios-sim-object/native_embed_sim.o \
  ./tool/build_journal_apple.sh ios-simulator
```

Platform `7` means iOS Simulator. The output is `_build/apple/ios-simulator/LogseqJournal.app`, bundle ID `com.logseq.journal`. The app needs an iOS 26 simulator.

```sh
xcrun simctl install <udid> _build/apple/ios-simulator/LogseqJournal.app
xcrun simctl launch <udid> com.logseq.journal
```

Without `JOURNAL_OCAML_OBJECT`, the build script links a stub instead of the OCaml core. The app builds but doesn't run.

## 4. Sign in and open a graph

1. The app opens on **Sign in to Logseq**, a native form. Enter `e2etest` / `Logseq-e2e`.
2. **Choose a graph** lists the account's graphs on `https://api.logseq.io`. The sync address is hardcoded in `swift/JournalNativeServices.swift` and must be HTTPS, so the journal can't use Chat's local server.
3. The test account's `rtc-…` graphs look like Logseq's shared test graphs. Browse them, but don't change them.

For encrypted graphs, per the repo notes:

- The first open may fail with a keychain miss. Tap **Retry** to get the "Unlock your graph" prompt, then enter the graph's encryption password.
- The graph then downloads and opens. Later launches restore it from a local mirror.
- A graph stuck at "Downloading graph" with the app idle won't recover. Delete its mirror folder and try another graph.

To sign in from a script, a small Maestro flow works:

```yaml
appId: com.logseq.journal
---
- tapOn: "Username"
- inputText: "e2etest"
- tapOn: "Password"
- inputText: "Logseq-e2e"
- tapOn: "Sign in"
```

## 5. Build the macOS app

The Mac build runs, but it can't sign in. Use it for UI work that doesn't need an account.

```sh
eval $(opam env --switch=journal-5.5.0)
dune build app/native_embed.exe.o
JOURNAL_OCAML_OBJECT=$PWD/_build/default/app/native_embed.exe.o ./tool/build_journal_apple.sh macos
open _build/apple/macos/LogseqJournal.app
```

- **Build break at `d4fbe23`.** Commit `e325255` put `.toolbarBackground(.hidden, for: .bottomBar)` outside the `#if os(iOS)` guard in `swift/JournalChrome.swift`. macOS doesn't have that API, so the build fails. Moving the line inside the guard fixes it.
- **Why login fails.** The login library needs keychain access groups. On macOS those need an embedded provisioning profile for `com.logseq.journal`, which only Logseq's team can issue. Ad hoc builds reach the sign-in form but fail with keychain error -34018. Signing with your own team (`JOURNAL_MACOS_TEAM_ID`) makes macOS refuse to launch the app, with error 163.

## 6. Diagnose problems

These tips come from the repo's agent skills and weren't verified for this guide.

- **The app logs only to stdout and stderr.** Nothing useful reaches the system log. Capture it with:

  ```sh
  xcrun simctl launch --console-pty <udid> com.logseq.journal > console.log
  ```

- **Find the app's data** with `xcrun simctl get_app_container <udid> com.logseq.journal data`. Look under `Library/Application Support/logseq-db-worker/synced-graphs/<graph-uuid>/db.sqlite`.
- **Check CPU to tell a hang from a loop.** `ps -o %cpu` near 0% means the app is idle-waiting. Near 100% means a render loop.
- **A blank screen with no errors** is usually a settled dead end, not a hang. The repo's `testing-ios-simulator` skill explains how to trace it.

## Known issues

| Issue | Status |
|---|---|
| `eio_posix` and `eio_main` not declared in any `.opam` file | Install by hand |
| macOS build broken by the `bottomBar` line (`e325255`) | One-line local fix |
| `source_boundary_test` fails since `316b159` | Upstream |
| macOS app can't sign in | Needs Logseq's provisioning profile |
| Sync address hardcoded to production | No local-server option |
| Repo notes: sheets can freeze on the iOS simulator; startup wakeup storm | Upstream, see `.agents/skills/` |
