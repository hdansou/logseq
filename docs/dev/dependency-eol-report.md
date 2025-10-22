# Logseq Dependencies: End of Life & Deprecation Report

**Report Generated:** 2025-10-21

## Executive Summary

This report provides a comprehensive overview of all major dependencies used in Logseq, including their current versions, End of Life (EOL) dates, and recommended upgrade priorities.

### Critical Findings

- **Electron 37.2.6** reaches EOL on January 13, 2026 (~3 months)
- **TypeScript 4.4.3** is 3+ years outdated with no security support
- **CodeMirror 5** is in legacy/maintenance mode

## Dependencies Overview

### Core Runtime & Build Tools

| Dependency     | Current Version | Latest Version | EOL/Deprecation Date | Status                | Priority | Notes                                                                       |
| -------------- | --------------- | -------------- | -------------------- | --------------------- | -------- | --------------------------------------------------------------------------- |
| **Node.js**    | ≥20.19.1        | 22.x LTS       | **April 30, 2026**   | 🟢 Active Maintenance | LOW      | In Maintenance LTS. Node 22 LTS supported until April 2027.                 |
| **Electron**   | 37.2.6          | 39.x           | **January 13, 2026** | 🔴 EOL in ~3 months   | URGENT   | Only supports latest 3 stable versions. Includes GPU fix for macOS Tahoe.   |
| **TypeScript** | 4.4.3           | 5.7+           | No official EOL      | 🔴 Very outdated      | HIGH     | Released Sep 2021. Security fixes backported ~1 year only. 3+ years behind. |
| **Webpack**    | 5.98.0          | 5.x            | No official EOL      | 🟢 Current            | -        | Webpack 5 is current major version. No v6 announced.                        |
| **Gulp**       | 4.0.2           | 4.x            | No official EOL      | 🟢 Current            | -        | Gulp 4 is current major version.                                            |
| **PostCSS**    | 8.4.47          | 8.x            | No official EOL      | 🟢 Current            | -        | PostCSS 8 is current major version.                                         |

### Database

| Dependency        | Current Version | Latest Version | EOL/Deprecation Date | Status       | Priority | Notes                                                                                                 |
| ----------------- | --------------- | -------------- | -------------------- | ------------ | -------- | ----------------------------------------------------------------------------------------------------- |
| **SQLite (WASM)** | 3.50.3          | 3.x (latest)   | **2050**             | 🟢 Excellent | -        | SQLite 3.x commitment through 2050. Only latest receives active support. Forward/backward compatible. |

### Clojure/ClojureScript Stack

| Dependency      | Current Version       | Latest Version | EOL/Deprecation Date | Status           | Priority | Notes                                             |
| --------------- | --------------------- | -------------- | -------------------- | ---------------- | -------- | ------------------------------------------------- |
| **Clojure**     | 1.11.1                | 1.12.x         | No official EOL      | 🟢 Supported     | -        | No formal EOL policy. 1.11 still viable.          |
| **Shadow-CLJS** | 2.28.23               | 3.2.0          | No official EOL      | 🟡 Behind latest | LOW      | Actively maintained, but using older 2.x version. |
| **DataScript**  | Custom fork (45f6721) | Custom fork    | N/A                  | 🟢 Maintained    | -        | Logseq team maintains fork.                       |
| **Rum**         | Custom fork (5d672bf) | Custom fork    | N/A                  | 🟢 Maintained    | -        | Logseq team maintains fork.                       |

### Frontend Framework & UI

| Dependency      | Current Version | Latest Version | EOL/Deprecation Date | Status       | Priority | Notes                                                                                           |
| --------------- | --------------- | -------------- | -------------------- | ------------ | -------- | ----------------------------------------------------------------------------------------------- |
| **React**       | 18.3.1          | 19.x           | No official EOL      | 🟢 Supported | -        | React 18 receives critical security fixes. No formal EOL. React 19 released Dec 2024.           |
| **React DOM**   | 18.3.1          | 19.x           | No official EOL      | 🟢 Supported | -        | Follows React versioning.                                                                       |
| **TailwindCSS** | 3.3.5           | 4.x            | No official EOL      | 🟡 Outdated  | LOW      | Only latest version supported. No security patches for 3.x. TailwindCSS 4 has breaking changes. |

### Code Editor & Content

| Dependency     | Current Version | Latest Version | EOL/Deprecation Date | Status                | Priority | Notes                                                                              |
| -------------- | --------------- | -------------- | -------------------- | --------------------- | -------- | ---------------------------------------------------------------------------------- |
| **CodeMirror** | 5.65.18         | 6.x            | No official EOL      | 🟡 Legacy/Maintenance | MEDIUM   | CM5 labeled "legacy". CM6 is actively maintained with better mobile/accessibility. |
| **KaTeX**      | 0.16.10         | 0.16.x         | No official EOL      | 🟢 Current            | -        | Actively maintained math rendering library.                                        |
| **Marked**     | 5.1.2           | 5.x/6.x        | No official EOL      | 🟢 Current            | -        | Actively maintained markdown parser.                                               |
| **PDF.js**     | 4.2.67          | 4.x            | No official EOL      | 🟢 Current            | -        | Mozilla-maintained, actively updated.                                              |
| **Excalidraw** | 0.16.1          | 0.17+          | No official EOL      | 🟢 Active             | -        | Frequently updated drawing library.                                                |

### Mobile (Capacitor)

| Dependency             | Current Version | Latest Version | EOL/Deprecation Date | Status     | Priority | Notes                                                             |
| ---------------------- | --------------- | -------------- | -------------------- | ---------- | -------- | ----------------------------------------------------------------- |
| **@capacitor/core**    | 7.2.0           | 7.2.0          | Active (latest)      | 🟢 Current | -        | Released Jan 2025. 6 months maintenance after next major release. |
| **@capacitor/android** | 7.2.0           | 7.2.0          | Active (latest)      | 🟢 Current | -        | Supports Android 15.                                              |
| **@capacitor/ios**     | 7.2.0           | 7.2.0          | Active (latest)      | 🟢 Current | -        | Supports iOS 18.                                                  |

### Testing & Development

| Dependency     | Current Version | Latest Version | EOL/Deprecation Date | Status     | Priority | Notes                                   |
| -------------- | --------------- | -------------- | -------------------- | ---------- | -------- | --------------------------------------- |
| **Playwright** | 1.51.0          | 1.51+          | No official EOL      | 🟢 Current | -        | Actively maintained, frequent releases. |

## JavaScript/ECMAScript Configuration

- **Target**: ESNext
- **Output Feature Set**: `:es-next-in` (ClojureScript compiler)
- **Module Format**: ESNext modules

## Legend

- 🔴 **Critical** - Requires immediate action
- 🟡 **Warning** - Plan upgrade path
- 🟢 **Good** - Currently supported/acceptable

## Detailed Priority Action Items

### 1. URGENT - Electron Upgrade (EOL: January 13, 2026)

**Current:** 37.2.6
**Target:** 38.x or 39.x
**Timeline:** ~3 months until EOL

**Reason:**

- Electron only supports the latest 3 stable major versions
- Version 37 will be unsupported when Electron 40 is released
- Includes important GPU resource usage fix for macOS Tahoe (PR #48376)

**Action Items:**

- [ ] Review Electron 38/39 breaking changes
- [ ] Test application with newer Electron version
- [ ] Update `static/package.json` and `resources/package.json`
- [ ] Run full QA testing on all platforms (macOS, Windows, Linux)
- [ ] Verify electron-rebuild compatibility

**Files to Update:**

- `static/package.json:58`
- `resources/package.json:58`

### 2. HIGH - TypeScript Upgrade

**Current:** 4.4.3
**Target:** 5.x (latest is 5.7+)
**Timeline:** As soon as feasible

**Reason:**

- Released September 2021 (3+ years old)
- Security fixes only backported ~1 year
- Missing many improvements and features from 5.x
- Potential security vulnerabilities

**Action Items:**

- [ ] Review TypeScript 5.x breaking changes
- [ ] Update tsconfig.json configurations
- [ ] Fix any type errors that arise from stricter checks
- [ ] Update `package.json` TypeScript dependency
- [ ] Run full build and test suite

**Files to Update:**

- `package.json:48`
- `libs/tsconfig.json`
- Other TypeScript configuration files

### 3. MEDIUM - CodeMirror 6 Migration

**Current:** 5.65.18
**Target:** 6.x
**Timeline:** Plan for future release cycle

**Reason:**

- CodeMirror 5 is labeled "legacy"
- CodeMirror 6 offers better mobile support, accessibility, and modern design
- CM6 is the actively maintained version

**Action Items:**

- [ ] Research CodeMirror 5 to 6 migration path
- [ ] Inventory all CodeMirror customizations and extensions
- [ ] Plan phased migration approach
- [ ] Test CM6 with Logseq's use cases
- [ ] Update all CodeMirror integrations

**Considerations:**

- This is a significant migration with potential breaking changes
- May require substantial refactoring
- Should be planned as part of a major release

### 4. LOW - TailwindCSS 4 Evaluation

**Current:** 3.3.5
**Target:** 4.x
**Timeline:** Evaluate for future release

**Reason:**

- TailwindCSS 4 released with new architecture
- No security patches for 3.x (only latest version supported)
- Performance improvements in v4

**Action Items:**

- [ ] Review TailwindCSS 4 breaking changes
- [ ] Assess migration effort vs. benefits
- [ ] Test build process with v4
- [ ] Evaluate impact on custom styles
- [ ] Plan migration timeline if beneficial

**Considerations:**

- TailwindCSS 4 has significant breaking changes
- May require CSS refactoring
- Evaluate ROI before committing

### 5. LOW - Shadow-CLJS Update

**Current:** 2.28.23
**Target:** 3.2.0
**Timeline:** Next minor release

**Reason:**

- Using older 2.x version while 3.x is available
- Actively maintained project

**Action Items:**

- [ ] Review Shadow-CLJS 3.x changelog
- [ ] Check for breaking changes
- [ ] Update `deps.edn` and `package.json`
- [ ] Test ClojureScript compilation
- [ ] Verify all build targets work correctly

**Files to Update:**

- `deps.edn:28`
- `package.json:42`

## Node.js Upgrade Consideration

**Current:** ≥20.19.1 (EOL: April 30, 2026)
**Recommended:** Consider Node.js 22 LTS (EOL: April 2027)

While Node.js 20 is still in active maintenance, planning an upgrade to Node.js 22 LTS would provide:

- Extended support timeline
- Better performance
- Newer JavaScript features
- Alignment with Electron's Node.js version

## Dependencies Without Formal EOL Policies

The following dependencies don't publish formal EOL dates but follow a "latest version only" support model:

- **DataScript** (custom fork) - Maintained by Logseq team
- **Rum** (custom fork) - Maintained by Logseq team
- **ClojureScript** (via Shadow-CLJS) - Actively maintained
- **React** - Receives critical security fixes for previous major version
- **Clojure** - No formal EOL policy
- **Webpack** - Actively maintained
- **PostCSS** - Actively maintained
- **PDF.js** - Mozilla-maintained
- **KaTeX** - Actively maintained
- **Marked** - Actively maintained
- **Excalidraw** - Frequently updated

## Logseq-Specific Findings

### GPU Resource Usage Issue (Electron)

**Issue:** Electron PR #48376 addresses a GPU resource usage problem on macOS 26 Tahoe where Electron's override of the private `_cornerMask` API causes excessive WindowServer GPU usage.

**Impact on Logseq:**

- Logseq does NOT directly use the problematic APIs (`_cornerMask`, `NSVisualEffectView`)
- The bug exists in Electron's internal implementation
- All Electron apps on macOS Tahoe are affected regardless of application code
- Users may experience system-wide GPU performance degradation

**Recommendation:**

- Upgrade to Electron 37.x patch version (when available) or Electron 38.x+
- This upgrade can be delayed if macOS Tahoe adoption is low among users
- Eventually required to prevent user impact

## Maintenance Recommendations

1. **Establish Dependency Update Cadence**

   - Monthly: Review security advisories
   - Quarterly: Update patch versions
   - Bi-annually: Evaluate major version upgrades

2. **Implement Dependency Monitoring**

   - Use tools like Dependabot or Renovate
   - Monitor GitHub Security Advisories
   - Subscribe to EOL notifications (endoflife.date)

3. **Testing Strategy**

   - Automated regression testing for dependency updates
   - Platform-specific testing (macOS, Windows, Linux, iOS, Android)
   - Performance benchmarking after major upgradesa

4. **Documentation**
   - Maintain this EOL report quarterly
   - Document upgrade decisions and rationale
   - Track known issues with specific dependency versions

## Resources

- [Node.js EOL](https://endoflife.date/nodejs)
- [Electron EOL](https://endoflife.date/electron)
- [React Versions](https://react.dev/versions)
- [TypeScript Support Policy](https://github.com/microsoft/TypeScript/blob/main/SUPPORT.md)
- [Capacitor Support Policy](https://capacitorjs.com/docs/main/reference/support-policy)
- [SQLite Long Term Support](https://sqlite.org/lts.html)

## Next Review Date

**Recommended:** January 2026 (before Electron 37 EOL)

---

_This report should be reviewed and updated quarterly to ensure Logseq maintains secure and supported dependencies._
