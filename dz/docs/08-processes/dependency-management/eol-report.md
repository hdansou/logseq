# Logseq Dependencies: End of Life & Deprecation Report

> **Last revised:** 2026-05-30

**Report Generated:** 2026-05-30

## Executive Summary

This report was revalidated against current repository manifests under `src/`, `deps/`, and `packages/`, plus root manifests used by those builds.

### Critical Findings

- **CodeMirror remains on v5 (`5.65.18`)** while v6 is the actively developed line.
- **TailwindCSS remains on v3 (`3.3.5`)** and should be periodically re-evaluated against v4 migration cost.
- **Electron is no longer on 38.x**; desktop packaging now targets **39.8.8**.
- **TypeScript is no longer on 4.x**; root and SDK manifests are on **5.9.3**.

## Dependencies Overview (Source-Validated)

### Core Runtime & Build Tools

| Dependency | Current Version in Source | Status | Evidence |
| ---------- | ------------------------- | ------ | -------- |
| **Node.js** | `>=22.20.0` | 🟢 Current baseline declared | `package.json` (`engines.node`) |
| **Electron** | `39.8.8` | 🟢 Updated from prior 38.x baseline | `resources/package.json` |
| **TypeScript** | `^5.9.3` (root), `5.9.3` (libs) | 🟢 Current in repo | `package.json`, `libs/package.json` |
| **Webpack** | `^5.106.2` (root), `5.105.4` (libs) | 🟢 Current major line | `package.json`, `libs/package.json` |
| **Gulp** | `^5.0.1` | 🟢 Current | `package.json` |
| **PostCSS** | `^8.5.13` (root), `^8.5.8` (`packages/ui`) | 🟢 Current major line | `package.json`, `packages/ui/package.json` |

### Database

| Dependency | Current Version in Source | Status | Evidence |
| ---------- | ------------------------- | ------ | -------- |
| **SQLite (WASM)** | `3.51.2-build9` | 🟢 Updated from prior 3.50.x entry | `package.json` (`@sqlite.org/sqlite-wasm`) |

### Clojure/ClojureScript Stack

| Dependency | Current Version in Source | Status | Evidence |
| ---------- | ------------------------- | ------ | -------- |
| **Clojure** | `1.12.4` | 🟢 Updated from prior 1.11.x entry | `deps.edn` |
| **Shadow-CLJS** | `3.4.4` (`deps.edn`), `^3.4.6` (`package.json`) | 🟢 Upgraded from prior 2.x entry | `deps.edn`, `package.json`, `deps/db-sync/package.json` |
| **DataScript** | Logseq fork (`3f141af...`) | 🟢 Maintained as fork | `deps.edn` |
| **Rum** | Logseq fork (`5d672bf...`) | 🟢 Maintained as fork | `deps.edn` |

### Frontend Framework & UI

| Dependency | Current Version in Source | Status | Evidence |
| ---------- | ------------------------- | ------ | -------- |
| **React** | `18.3.1` (exact root), `^18.3.1` (`packages/ui`) | 🟢 Stable baseline | `package.json`, `packages/ui/package.json` |
| **React DOM** | `18.3.1` (exact root), `^18.3.1` (`packages/ui`) | 🟢 Stable baseline | `package.json`, `packages/ui/package.json` |
| **TailwindCSS** | `3.3.5` | 🟡 Still on v3 | `package.json` |

### Code Editor & Content

| Dependency | Current Version in Source | Status | Evidence |
| ---------- | ------------------------- | ------ | -------- |
| **CodeMirror** | `5.65.18` | 🟡 Legacy line retained | `package.json` |
| **KaTeX** | `^0.16.45` | 🟢 Updated from prior 0.16.10 entry | `package.json` |
| **Marked** | `^17.0.6` | 🟢 Updated from prior 5.x entry | `package.json` |
| **PDF.js** | `4.2.67` | 🟢 Current in source | `package.json` (`pdfjs-dist`) |

### Mobile (Capacitor)

| Dependency | Current Version in Source | Status | Evidence |
| ---------- | ------------------------- | ------ | -------- |
| **@capacitor/core** | `^8.3.1` | 🟢 Major version uplift vs prior v7 entry | `package.json` |
| **@capacitor/android** | `^8.3.1` | 🟢 Major version uplift vs prior v7 entry | `package.json` |
| **@capacitor/ios** | `^8.3.1` | 🟢 Major version uplift vs prior v7 entry | `package.json` |

### Testing & Development

| Dependency | Current Version in Source | Status | Evidence |
| ---------- | ------------------------- | ------ | -------- |
| **Playwright** | `^1.58.2` | 🟢 Updated from prior 1.51.x entry | `package.json` |

## Findings: New Capabilities

- **Capacitor 8 adoption** (`@capacitor/core/android/ios` at `^8.3.1`) indicates the mobile stack has moved to the newer major runtime and plugin ecosystem.
- **TypeScript 5.9.3 baseline** (root + SDK) enables modern TS features and improved type tooling versus the previously documented 4.x state.
- **Shadow-CLJS 3.4.x baseline** (Clojure deps + npm tooling) reflects a modernized CLJS toolchain compared to the prior 2.x entry.
- **Electron 39.8.8** in desktop packaging manifests indicates post-38 migration is complete.

## Removed / No Longer Accurate Items

> **Removed:** "Electron 38.4.0 is EOL and upgrade to 39.x is overdue." Source now shows Electron `39.8.8` in desktop packaging.

> **Removed:** "TypeScript 4.4.3 is 3+ years outdated." Source now shows TypeScript `^5.9.3` / `5.9.3`.

> **Removed:** Prior "COMPLETED - Electron Upgrade" subsection pointing to `static/package.json` updates. This file is not present in current repo layout; desktop Electron dependency is tracked in `resources/package.json`.

> **Removed:** Prior "HIGH - TypeScript Upgrade" action block. Upgrade is already landed in source manifests.

> **Removed:** `Excalidraw` dependency row from this report because no `excalidraw` package declaration exists in current manifest set scanned for this validation pass.

## JavaScript/ECMAScript Configuration

- **Output Feature Set**: `:es-next-in` (present in multiple builds in `shadow-cljs.edn`)
- **Build Targets in Source**: browser + node-script builds (`:app`, `:mobile`, `:electron`, `:db-worker`, etc.)

## Priority Actions (Current)

1. **MEDIUM - CodeMirror 6 migration evaluation**
   - Current source still depends on `codemirror` `5.65.18`.
   - Keep migration planning on roadmap.

2. **LOW - TailwindCSS v4 evaluation**
   - Current source uses `tailwindcss` `3.3.5`.
   - Reassess migration effort and compatibility with existing styles.

3. **LOW - Version normalization for Shadow-CLJS declarations**
   - `deps.edn` uses `3.4.4` while npm manifests declare `^3.4.6`.
   - Optional alignment task for tooling consistency.

## Next Review Date

**Recommended:** 2026-08-30 (quarterly cadence)

---

_This report should be reviewed and updated quarterly to keep dependency risk and lifecycle guidance aligned with source-of-truth manifests._
