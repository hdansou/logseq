# Logseq iOS Development Investigation — Part 3

## Sync Architecture & Self-Hosted Enablement

**Investigation Date:** February 19, 2026  
**Status:** Developer-focused sync implementation and testing guide

---

## Executive Summary

Logseq mobile sync for modern graphs is based on RTC-style synchronization over WebSocket with encrypted payloads and client-side sync logic in worker code.

Self-hosted/local sync is feasible for development through the db-sync local setup path, but production-grade self-hosted guidance is still a documentation gap.

---

## 1) Sync Modes Relevant to iOS

| Sync Path                       | Graph Mode       | iOS Support      | Practical Notes                                |
| ------------------------------- | ---------------- | ---------------- | ---------------------------------------------- |
| RTC/Logseq Sync                 | Database graph   | ✅               | Primary modern mobile sync path                |
| File cloud/manual sync          | File-based graph | ⚠️               | Usable, but conflict handling needs discipline |
| Git sync                        | File-based graph | ❌               | Not available on iOS                           |
| Self-hosted local db-sync (dev) | Database graph   | ✅ (dev testing) | Requires local env + endpoint override         |

---

## 2) Architecture Overview

High-level flow:

1. Frontend graph mutations are observed
2. Sync operations are assembled and versioned
3. Worker sync client communicates over WebSocket
4. Remote operations are applied locally with conflict-handling logic

Core code locations:

- `src/main/frontend/worker/rtc/core.cljs`
- `src/main/frontend/worker/rtc/client.cljs`
- `src/main/frontend/worker/rtc/ws.cljs`
- `src/main/frontend/worker/rtc/remote_update.cljs`
- `src/main/frontend/worker/rtc/asset.cljs`
- `src/main/frontend/worker/rtc/db_listener.cljs`

---

## 3) Encryption & Security Notes

The RTC path is designed around end-to-end encryption patterns (see dedicated security investigation for crypto detail).

- Transport uses secure websocket endpoints in normal hosted mode (`wss://`)
- Key/crypto handling exists in sync-related worker and mobile native glue paths
- iOS native code includes sync-related crypto utility implementation under `ios/App/App/FileSync/`

For deep crypto review, use: `dz/docs/07-investigations/rtc-e2ee-security-investigation.md`.

---

## 4) Enabling Local/Self-Hosted Sync for Development

Reference baseline: `deps/db-sync/test-db-sync-local.md`

### Typical local flow

1. Build local sync adapter/server
2. Start local sync service with chosen auth driver
3. Run app in a mode that targets local sync endpoint
4. Verify graph operations sync across clients

Representative environment patterns from local docs include:

- `ENABLE_DB_SYNC_LOCAL=true`
- local endpoint on `http://localhost:8787`
- websocket path format such as `ws://127.0.0.1:8787/sync/%s`

### Auth options in local/dev contexts

- `DB_SYNC_AUTH_DRIVER=none` for fully local, no-validation iteration
- `DB_SYNC_AUTH_DRIVER=static` for simple token-based local testing

Use no-auth mode only in isolated development/testing.

---

## 5) iOS-Specific Sync Constraints

- App lifecycle/background limits on iOS can pause active sync while app is not foregrounded
- Network transitions (offline/online, cellular/wifi switches) are common mobile realities
- Retry/reconnect behavior must be validated on real devices, not simulator-only

These constraints explain why sync reliability testing should include app suspend/resume and flaky-network scenarios.

---

## 6) How to Validate Mobile Sync Changes

- [ ] Test with two clients against same graph (desktop + iOS, or iOS + iOS)
- [ ] Confirm update/move/delete operations converge
- [ ] Test offline edit and reconnect behavior
- [ ] Test app background + foreground transitions
- [ ] Test at least one large graph and one asset-heavy graph

---

## 7) Self-Hosted Production Readiness Notes

Current repository documentation is strong for local/dev self-hosting, but production runbook details are incomplete.

Still needed for full production self-hosted guidance:

- TLS/proxy deployment architecture
- durable auth/identity integration guidance
- observability, backup, and disaster recovery standards
- explicit mobile compatibility SLOs and support boundaries

---

## 8) References

- Local db-sync testing: `deps/db-sync/test-db-sync-local.md`
- RTC onboarding: `dz/docs/01-onboarding/rtc-onboarding.md`
- RTC implementation code: `src/main/frontend/worker/rtc/`
- Crypto investigation: `dz/docs/07-investigations/rtc-e2ee-security-investigation.md`
- Investigation index: `ios-development-00-index.md`
