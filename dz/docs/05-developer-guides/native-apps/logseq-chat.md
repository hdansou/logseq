# Logseq Chat: Build and Test

> **Last revised:** 2026-09-27 · verified at chat `7a40565`, db-sync `feature/native-mobile-app` at `148d042`

Logseq Chat is "Logseq mobile v2": a native SwiftUI app for iOS and a Flutter app for Android, sharing one OCaml core. This guide covers the iOS simulator. Read the [overview](README.md) first for prerequisites and the test account.

## Contents

1. [Set up the OCaml environment](#1-set-up-the-ocaml-environment)
2. [Run the core tests](#2-run-the-core-tests)
3. [Build the iOS simulator app](#3-build-the-ios-simulator-app)
4. [Run a local sync server](#4-run-a-local-sync-server)
5. [Try the app by hand](#5-try-the-app-by-hand)
6. [Run the UI tests](#6-run-the-ui-tests)
7. [Troubleshooting](#troubleshooting)

## 1. Set up the OCaml environment

This follows `.github/workflows/ci.yml`. Run it from the repo root with the SSH-to-HTTPS rewrite from the overview exported.

```sh
git clone https://github.com/logseq/chat.git && cd chat
opam switch create 5.5.0 --packages=ocaml-base-compiler.5.5.0 -y
eval $(opam env --switch=5.5.0)

opam pin add -n -y melange-edn-core       git+https://github.com/logseq/melange-edn.git#main
opam pin add -n -y melange-edn-native     git+https://github.com/logseq/melange-edn.git#main
opam pin add -n -y melange-transit-core   git+https://github.com/logseq/melange-transit.git#main
opam pin add -n -y melange-transit-native git+https://github.com/logseq/melange-transit.git#main
opam install . --deps-only --yes --with-test
```

If opam asks to install system packages, it needs `libffi` and `pkgconf` from Homebrew.

Then remove the switch copies of vendored packages, add the two tools CI adds, and vendor the locked sources into `duniverse/`:

```sh
comm -12 \
  <(grep -oE '"[a-zA-Z0-9_.-]+" \{= "[^"]*" & \?vendor\}' logseq_chat.opam.locked | cut -d'"' -f2 | sort) \
  <(opam list --installed --short | sort) | xargs opam remove -y
opam install -y melange.7.0.1-55
opam install -y opam-monorepo.0.3.6

opam exec -- opam-monorepo pull
# Clone the logseq/* pins that the lockfile doesn't vendor, at their pinned commits:
for pin in $(grep -oE 'logseq/[A-Za-z0-9_-]+\.git#[A-Za-z0-9._/-]+' logseq_chat.opam | sort -u); do
  url="${pin%%#*}"; sha="${pin##*#}"; name="$(basename "$url" .git)"
  [ -d "duniverse/$name" ] && continue
  git clone --quiet "https://github.com/$url" "duniverse/$name"
  git -C "duniverse/$name" checkout --quiet "$sha" \
    || { git -C "duniverse/$name" fetch --quiet "https://github.com/$url" "$sha" \
         && git -C "duniverse/$name" checkout --quiet FETCH_HEAD; }
done
```

A later `opam-monorepo pull` wipes the manually cloned folders, so rerun the loop after each pull.

## 2. Run the core tests

The core tests cover the app logic, the data model and scripted user flows. They need no simulator and no server.

```sh
opam exec --switch=5.5.0 -- dune build @shared/native/runtest
```

Expected result: `Test Successful … 720 tests run.`

Don't run `swift test`. The repo's notes say its XCTest bridge hangs.

## 3. Build the iOS simulator app

```sh
LOGSEQ_CHAT_OPAM_SWITCH=5.5.0 ./scripts/build-mobile-ios-simulator.sh
```

- The first run builds an OCaml 5.5.0 cross-compiler for iOS under `_build/apple-toolchains/`. That takes a while; later builds reuse it.
- The output is `apple/.build/LogseqChat.app`, bundle ID `com.logseq.chat`. A good run ends with "Verified four distinct app shortcuts and the embedded widget extension".
- The script can exit early or with a misleading status. Check the log, and confirm `apple/.build/LogseqChat.app/LogseqChat` exists.

Install and launch it:

```sh
UDID=<simulator udid>
xcrun simctl boot $UDID; open -a Simulator
xcrun simctl install $UDID apple/.build/LogseqChat.app
xcrun simctl launch $UDID com.logseq.chat
```

The app opens on a "Logseq Chat" screen with a **Sign in** button.

## 4. Run a local sync server

The app talks to db-sync at `http://127.0.0.1:8787`. It has to be built from the logseq branch `feature/native-mobile-app`. Master's db-sync lacks `schema-version` in the snapshot download, and "Add sync graph" then fails.

A detached worktree keeps it separate from your other checkouts:

```sh
cd <your logseq checkout>
git fetch upstream feature/native-mobile-app
git worktree add --detach ../logseq-native-mobile-app upstream/feature/native-mobile-app
cd ../logseq-native-mobile-app
pnpm install
cd deps/db-sync
clojure -M:cljs release db-sync-node
mkdir -p data
DB_SYNC_PORT=8787 DB_SYNC_DATA_DIR="$PWD/data" ./start.sh
```

- It needs Node 22, pnpm, the Clojure CLI and Java. CI uses Java 21; Java 26 also built it.
- It's ready when the log says "Logseq sync listening on port 8787". A request to `/` returns 404, which is fine.
- It still verifies logins against the real development Cognito pool, so you need network access.
- Stop it with `lsof -ti tcp:8787 | xargs kill`.
- Graphs pile up in `data/` across runs. For a clean slate, stop the server and empty that folder.

## 5. Try the app by hand

1. With the server running, tap **Sign in** and use `e2etest` / `Logseq-e2e`.
2. Choose **Add sync graph**, or pick an existing graph.
3. Today's journal opens.

Things to try:

| Control | What it does |
|---|---|
| Capture bar, bottom | Adds a block to today's journal |
| Tap a block | Edits it in the outliner, with a toolbar for move, tag, camera, audio and links |
| Type `[[name` | Links a page. If nothing matches, pick **New page: name** to create it |
| Type `#name` | Tags a block. Pick **New tag: name** to create one |
| Menu, top left | Sidebar with pages and graphs |
| Orange dot, top right | Sync status: connection, local changes, **Sync now** |
| Search, bottom right | Searches the graph |

There is no separate "New page" button. Pages are created by linking to them.

## 6. Run the UI tests

The UI tests are 29 Maestro flows in `tests/e2e/`. They cover capture, outliner editing, the sidebar, settings and graph sync.

1. Install Maestro. Its installer adds a PATH line to `~/.zshrc` and `~/.bash_profile`.

   ```sh
   curl -fsSL https://get.maestro.mobile.dev | bash
   ```

2. Start the local sync server (section 4).
3. Boot a simulator that nobody will touch during the run.
4. Run a group of flows:

   ```sh
   export PATH="$PATH:$HOME/.maestro/bin"
   eval $(opam env --switch=5.5.0)
   export LOGSEQ_CHAT_OPAM_SWITCH=5.5.0 DUNE="$(command -v dune)" \
     LOGSEQ_CHAT_IOS_SKIP_BUILD=1 LOGSEQ_CHAT_IOS_SIMULATOR_UDID=<udid>
   ./scripts/test-ios-e2e-suite.sh smoke
   ```

| Group | Flows |
|---|---|
| `smoke` | 4: capture, cold-start composer, search status, sidebar |
| `composer` | 3 capture and send flows |
| `outliner` | outliner editing, toolbar, autocomplete, drag |
| `navigation`, `settings`, `content`, `graphs` | the named areas |
| `all` | all 29 |

Run `./scripts/test-ios-e2e-suite.sh --list` for the exact list. To run one flow: `LOGSEQ_CHAT_IOS_E2E_FLOW=tests/e2e/<flow>.yaml ./scripts/test-ios-e2e.sh`.

How a run behaves:

- **Each flow starts clean.** The script uninstalls the app or wipes its data, points it at the local server, and signs in as the test user.
- **Some flows seed a fixture graph first.** Set `LOGSEQ_CHAT_E2E_SEED_CACHE=<dir>` to reuse seeded graphs across runs.
- **The suite stops at the first failure.** Maestro saves screenshots and the screen hierarchy under `~/.maestro/tests/<timestamp>/`.
- **Expect about ten minutes for the first flow**, including seeding. The full suite takes well over an hour.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `Permission denied (publickey)` fetching lui | SSH pin, no GitHub SSH key | Export the SSH-to-HTTPS rewrite (overview) |
| opam: `Missing dependency: lui, unknown package` | lui's SSH pin failed to fetch | Same as above |
| A flow fails with odd content, such as text the test never typed | Someone used the simulator mid-run | Rerun on an untouched simulator |
| Screen reported as landscape during a flow | Simulator window rotated mid-run | Same as above |
| Setup finds a sync-status sheet or an older graph instead of "Choose a graph" | Leftover graphs on the local server, suspected | Empty the server's `data/` folder and retry |
| "Add sync graph" fails with `NSURLErrorDomain -1017` | Server built from master | Build db-sync from `feature/native-mobile-app` |
| Core crash writing `logseq-chat-initial-*.snapshot` | App container `tmp/` missing | The test script recreates it; recreate it by hand when testing manually |

The repo's `AGENTS.md` says the iOS build needs an `ocaml-demo` checkout. No script uses it anymore.
