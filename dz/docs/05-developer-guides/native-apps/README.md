# Native Apps: Logseq Chat, Logseq Journal and lui

> **Last revised:** 2026-09-27 · verified on macOS 26 (Apple silicon), Xcode 26.6

Logseq is building a new generation of native apps in OCaml. None of them has shipped. This section explains what the apps are, how they relate, and how to build and test them on a Mac.

- [Logseq Chat: build and test](logseq-chat.md)
- [Logseq Journal: build and test](logseq-journal.md)

## What the repositories are

| Repository | What it is | Platforms |
|---|---|---|
| [logseq/lui](https://github.com/logseq/lui) | A cross-platform UI runtime. App code, state and diffing stay in OCaml; each backend applies patches to native UI objects. | SwiftUI (iOS, macOS), Flutter (Android), web DOM via Melange |
| [logseq/chat](https://github.com/logseq/chat) | "Logseq mobile v2": quick capture, journals, an outliner mode, widgets and shortcuts. Keeps a local copy of a graph and syncs it with db-sync. | iOS (SwiftUI), Android (Flutter) |
| [logseq/logseq_journal](https://github.com/logseq/logseq_journal) | A journal-focused app with its own DB worker, sync client and storage packages. No README yet. | iOS, macOS (SwiftUI); Flutter code exists |

All three are public, were created between August and September 2026, and change daily.

### How they fit together

- Both apps keep their logic in OCaml and draw through lui. Only the last step, drawing, is platform-specific.
- Both use Logseq's OCaml libraries: `datascript-ocaml`, `ocaml-signal`, `melange-edn` and `melange-transit`.
- Chat's sync endpoints live on the logseq branch `feature/native-mobile-app` (db-sync). They are not on master.
- The main logseq repo is moving the same way: the CLI already ships in OCaml, and an OCaml DB worker is in review ([logseq PR #13411](https://github.com/logseq/logseq/pull/13411)).

## Status on this machine

Checked on 2026-09-27.

| App | Target | Build | Tests | Sign-in |
|---|---|---|---|---|
| Chat `7a40565` | iOS simulator | Works | Core: 720 passed. UI: one flow passed; full suite not yet run undisturbed | Works against a local db-sync |
| Chat | Android | Not tried | Not tried | Not tried |
| Journal `d4fbe23` | macOS | Works, with a one-line local fix | Core: 21 of 22 suites pass | Fails; keychain needs a provisioning profile |
| Journal | iOS simulator | Works | Not run | Works against `api.logseq.io` |

## Shared prerequisites

1. **Xcode** with an iOS 26 simulator runtime. The journal targets iOS 26.0; Chat targets iOS 17.0.
2. **Homebrew packages:**

   ```sh
   brew install opam libffi pkgconf
   opam init -y --bare --no-setup   # --no-setup leaves your shell profile alone
   ```

3. **Separate opam switches.** opam pins apply to a whole switch, and the two apps pin different commits of the same libraries. Give each app its own switch:

   | App | Switch |
   |---|---|
   | Chat | `5.5.0` (the name its scripts expect) |
   | Journal | `journal-5.5.0` |

   Create one with `opam switch create <name> --packages=ocaml-base-compiler.5.5.0 -y`. Each takes several minutes to compile.

4. **Git over HTTPS.** Some pins use SSH URLs such as `ssh://git@github.com/logseq/lui.git`. Every pinned repo is public, so without a GitHub SSH key, rewrite SSH to HTTPS for the build, as the repos' CI does:

   ```sh
   export GIT_TERMINAL_PROMPT=0 GIT_CONFIG_COUNT=2 \
     GIT_CONFIG_KEY_0=url.https://github.com/.insteadOf GIT_CONFIG_VALUE_0=git@github.com: \
     GIT_CONFIG_KEY_1=url.https://github.com/.insteadOf GIT_CONFIG_VALUE_1=ssh://git@github.com/
   ```

   Export this in any shell that runs `opam install` or an app build script. It also covers Swift Package Manager.

## Test account

Both apps sign in through Logseq's **development** Cognito user pool (`us-east-1_dtagLnju8`). The shared test account is the default in Chat's test scripts:

| Username | Password |
|---|---|
| `e2etest` | `Logseq-e2e` |

- In **Chat**, the account's graphs live on your local db-sync server, so you can do anything with them.
- In **Journal**, the account lists graphs named `rtc-graph-…` and `rtc-extra-test-graph-…` on Logseq's real sync server. They look like graphs Logseq's automated tests use. Browse them, but don't edit or delete them.

## Simulator hygiene

- **Give automated tests their own simulator.** Chat's UI tests wipe the app's data before each flow. If you tap or rotate that simulator during a run, flows fail.
- **Use explicit device IDs.** With several simulators booted, `booted` in `simctl` commands is ambiguous. List devices with `xcrun simctl list devices | grep Booted`.
- **Quitting Simulator doesn't uninstall apps.** They and their data survive a shutdown.
