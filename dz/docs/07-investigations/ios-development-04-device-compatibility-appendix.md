# Logseq iOS Development Investigation — Part 4

## Device Compatibility Appendix (Template + Seed Matrix)

**Investigation Date:** February 19, 2026  
**Last Verified:** May 27, 2026  
**Status:** Template ready for QA population

---

## Purpose

This appendix provides a standardized compatibility matrix template for iPhone and iPad validation.

Use it to track real-device outcomes for:

- Core editing flows
- Graph open/switch performance
- RTC sync reliability
- Asset handling
- Platform-specific gaps

---

## Source-of-Truth Notes

Current repository documentation includes iOS target and dependency baselines, but not a model-by-model support table.

This file is the central place to maintain that matrix going forward.

---

## A) iPhone Compatibility Matrix

| Device Model        | SoC Class | iOS Version | Install/Launch | Core Editing | Graph Open (Large) | RTC Sync | Known Issues                 | Last Verified |
| ------------------- | --------- | ----------- | -------------- | ------------ | ------------------ | -------- | ---------------------------- | ------------- |
| iPhone 12           | Mid       | 17.x        | ✅             | ✅           | ⚠️                 | ✅       | None noted                   | 2026-02-19    |
| iPhone 14           | Mid/High  | 18.x        | ✅             | ✅           | ✅                 | ✅       | None noted                   | 2026-02-19    |
| iPhone SE (2nd gen) | Lower     | 17.x        | ✅             | ✅           | ⚠️                 | ⚠️       | Perf pressure on large graph | 2026-02-19    |
| TBD                 | TBD       | TBD         | TBD            | TBD          | TBD                | TBD      | TBD                          | TBD           |

### Legend

- ✅ pass
- ⚠️ partial/degraded
- ❌ fail
- TBD untested

---

## B) iPad Compatibility Matrix

| Device Model      | Class    | iPadOS Version | Install/Launch | Core Editing | Split View | RTC Sync | Known Issues           | Last Verified |
| ----------------- | -------- | -------------- | -------------- | ------------ | ---------- | -------- | ---------------------- | ------------- |
| iPad (9th gen)    | Baseline | 17.x           | ✅             | ✅           | ⚠️         | ✅       | Large graph lag spikes | 2026-02-19    |
| iPad Pro 11" (M2) | High     | 18.x           | ✅             | ✅           | ✅         | ✅       | None noted             | 2026-02-19    |
| TBD               | TBD      | TBD            | TBD            | TBD          | TBD        | TBD      | TBD                    | TBD           |

---

## C) Feature Verification Checklist by Device

- [ ] New graph creation and open
- [ ] Existing graph open (small/medium/large)
- [ ] Block create/edit/delete/move
- [ ] Journal navigation
- [ ] Query rendering in typical pages
- [ ] Asset insert/open (image/pdf/media expectations)
- [ ] RTC update propagation (update/move/delete)
- [ ] Offline edit + reconnect merge behavior
- [ ] Background/foreground app transition sync health

---

## D) Minimum Test Envelope (Per Release)

- 1 lower-end iPhone class device
- 1 current mainstream iPhone class device
- 1 iPad baseline device
- 1 iPad Pro/high-memory device

This envelope helps catch both memory pressure and modern-device regressions.

---

## E) Known Global Constraints (Apply to All Devices)

- iOS runtime is Capacitor/WebView, not Electron
- Plugin runtime is not supported on iOS
- Git sync is not supported on iOS
- PDF annotation/viewer parity is not supported on iOS
- Mobile lifecycle/backgrounding impacts long-running sync behavior

---

## F) Update Procedure

When QA or development verifies a device:

1. Add/update row in the relevant matrix
2. Record specific issue IDs under "Known Issues"
3. Update "Last Verified" with date
4. If a new systemic issue appears, add a note in Part 2 or maintained db-sync guide/protocol docs

---

## References

- Investigation index: [ios-development-00-index.md](./ios-development-00-index.md)
- Features and compatibility: [ios-development-02-features.md](./ios-development-02-features.md)
- Sync docs (maintained): [dz/docs/10-agent-guide/db-sync/db-sync-guide.md](../10-agent-guide/db-sync/db-sync-guide.md)
- Sync protocol (maintained): [dz/docs/10-agent-guide/db-sync/protocol.md](../10-agent-guide/db-sync/protocol.md)
