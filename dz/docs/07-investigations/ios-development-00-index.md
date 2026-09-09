# Logseq iOS Development Investigation

## Investigation Index & Quick Reference

**Investigation Date:** February 19, 2026  
**Last Verified:** May 27, 2026  
**Audience:** New developers onboarding to Logseq iOS app work  
**Scope:** Setup, feature scope, compatibility matrix, sync and self-hosted sync enablement

---

## Executive Summary

This investigation maps what a new developer needs to ship and debug the Logseq iOS app:

- How to set up and build the iOS app quickly and reliably
- What feature set is in scope on iOS (and what is explicitly out of scope)
- The current compatibility matrix (iOS versions, platform constraints, plugin/runtime limitations)
- How sync works, including local/self-hosted sync enablement for development

The most important architectural constraint is that iOS runs inside Capacitor/WebView, not Electron. That directly explains key feature differences (for example: no plugin runtime and no PDF annotation support on iOS).

---

## Document Series

### [Part 1: Setup & Build Pipeline](ios-development-01-setup.md)

**Purpose:** Environment setup, local development loop, build/release flow, and iOS-specific native project details.

### [Part 2: Features in Scope & Compatibility Matrix](ios-development-02-features.md)

**Purpose:** Supported/limited/unsupported feature matrix, runtime constraints, and compatibility table for new development planning.

### [Part 3: Sync Architecture & Self-Hosted Enablement](ios-development-03-sync.md)

**Purpose:** RTC sync architecture, mobile sync behavior, and concrete steps to run against self-hosted/local sync endpoints.

### [Part 4: Device Compatibility Appendix](ios-development-04-device-compatibility-appendix.md)

**Purpose:** Ready-to-use iPhone/iPad compatibility matrix template for QA and release validation.

---

## Fast Start Reading Path

- **Day 1 onboarding:** Read Part 1 first, then Part 2
- **Building a mobile feature:** Read Part 2 before implementation
- **Working on sync or collaboration:** Read Part 3 in full
- **Preparing release QA:** Use Part 4 device matrix template

---

## Core Code & Config Entry Points

- Mobile development guide: [docs/develop-logseq-on-mobile.md](../../../docs/develop-logseq-on-mobile.md)
- Capacitor config: [capacitor.config.ts](../../../capacitor.config.ts)
- iOS native project: [ios/App/](../../../ios/App/)
- Platform detection helpers: [src/main/frontend/mobile/util.cljs](../../../src/main/frontend/mobile/util.cljs)
- Sync worker implementation: [src/main/frontend/worker/sync/](../../../src/main/frontend/worker/sync/)
- Local/self-hosted sync test guide: [dz/scripts/self-host/test-db-sync-local.md](../../scripts/self-host/test-db-sync-local.md)

---

## Known Gaps (Still Needing Follow-up)

- Device-model test matrix is captured in this investigation series (Part 4), but not yet promoted to a canonical maintained doc.
- Production self-hosted sync deployment runbook is not documented in-repo
- iOS version deprecation/support lifecycle policy is not explicitly documented
