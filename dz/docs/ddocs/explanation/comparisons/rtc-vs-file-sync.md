---
title: 'RTC vs File Sync'
description: 'Understanding the differences between File Sync and Real-Time Collaboration synchronization methods'
status: stable
lastVerified: '2026-05-30'
logseqVersion: '0.11.x'
category: comparisons
relatedDocs:
  tutorials:
    - '[Real-Time Sync Setup](../../tutorials/collaboration/real-time-sync.md)'
  explanation:
    - '[E2EE Encryption](../security/e2ee-encryption.md)'
---

# RTC vs File Sync

> **Last revised:** 2026-05-30

> Understanding the two synchronization methods in Logseq and when to use each.

## Overview

Historically, Logseq had two cloud sync approaches. In the current codebase, RTC/db-sync is the active sync implementation.

> **Removed:** File Sync (Markdown cloud sync) is no longer active in current source. The old frontend file-sync runtime path has been removed (for example, `src/main/frontend/fs/sync.cljs` no longer exists). Remaining `/file-sync/` API usage in `src/main/frontend/handler/user.cljs` is used for account/quota checks rather than a full Markdown sync engine.

Historical comparison context:

| Method        | Graph Type      | Sync Speed      | Collaboration   |
| ------------- | --------------- | --------------- | --------------- |
| **File Sync** | Markdown files  | Periodic (~10s) | Limited         |
| **RTC**       | Database graphs | Real-time       | Full multi-user |

---

## How They Work

### File Sync (Markdown)

```
Device A          Cloud Storage         Device B
   │                    │                   │
   │──── Upload ────────►                   │
   │    (.md files)     │                   │
   │                    │──── Download ─────►
   │                    │    (.md files)    │
```

**Characteristics:**

- Syncs entire files when changes detected
- ~10 second polling interval
- Conflicts create `conflict-*.md` files
- Works with any external Markdown editor

> **Removed:** This section describes historical behavior. The current source tree does not include the old file-sync runtime implementation.

### RTC (Real-Time Collaboration)

```
Device A          RTC Server           Device B
   │                    │                   │
   │══ WebSocket ═══════╪═══ WebSocket ═════│
   │                    │                   │
   │── Operation ──────►│──── Operation ────►
   │◄── Operation ──────│◄─── Operation ────│
```

**Characteristics:**

- Syncs operations (not files)
- Near real-time updates via WebSocket + pull/changed cycles
- Automatic reconciliation with reverse-apply/rebase of local pending txs
- Block-level editing presence awareness

---

## Feature Comparison

| Feature                   | File Sync           | RTC          |
| ------------------------- | ------------------- | ------------ |
| **Graph type required**   | Markdown            | Database     |
| **Real-time sync**        | ❌ No               | ✅ Yes       |
| **Multi-user editing**    | ⚠️ Poor (conflicts) | ✅ Excellent |
| **Conflict resolution**   | Manual              | Automatic    |
| **External editors**      | ✅ Yes              | ❌ No        |
| **Offline support**       | ✅ Yes              | ✅ Yes       |
| **Asset sync**            | ✅ Yes              | ✅ Yes       |
| **Version history**       | ✅ Yes              | ✅ Yes       |
| **Bandwidth usage**       | Higher              | Lower        |
| **End-to-end encryption** | ❌ No               | ✅ Yes       |

> **Removed:** File Sync feature cells in this table are historical-only and no longer represent an active cloud sync mode in current source.

---

## Size Limits

### File Sync Limits

| Resource       | Limit             | Reason                |
| -------------- | ----------------- | --------------------- |
| Per-file size  | 100 MB            | Transfer optimization |
| Total graph    | Subscription tier | Storage quota         |
| Batch upload   | 20 files          | Rate limiting         |
| Batch download | 100 files         | Memory management     |

> **Removed:** File Sync limits are historical. Current source no longer implements this Markdown cloud sync pipeline.

### RTC Limits

| Resource          | Limit                        | Reason                      |
| ----------------- | ---------------------------- | --------------------------- |
| Per-asset size    | 100 MB                       | S3 upload limits            |
| Flush batch       | 50 txs                       | Per flush cycle             |
| Total graph       | Service/account limits apply | Remote quota + graph limits |

## Findings: New Capabilities

Compared with older File Sync vs RTC writeups, current RTC/db-sync source includes additional concrete capabilities:

- Presence payloads include editing block context (`presence` + `editing-block-uuid`) and online user updates (`online-users`).
- Tx rejection includes richer metadata (`success-tx-ids`, `failed-tx-id`) for partial batch outcomes.
- Remote tx apply path rebases local pending changes and tracks sync conflicts (stored and broadcast via `:sync-conflicts-updated`).
- E2EE key-management endpoints include user key pair APIs and graph access-grant flow (`/e2ee/.../grant-access`).

---

## When to Use Each

### Choose File Sync When:

✅ You want to edit files with external tools (VS Code, Vim, etc.)  
✅ You use Git for version control  
✅ You're the only user of the graph  
✅ You prefer plain text files  
✅ You need maximum portability

> **Removed:** This recommendation is historical. Current active cloud sync implementation is RTC/db-sync.

### Choose RTC When:

✅ Multiple people edit simultaneously  
✅ You need instant sync across devices  
✅ You want E2EE encryption  
✅ You use advanced database features (typed properties, classes)  
✅ You have a very large knowledge base

---

## Conflict Handling

### File Sync Conflicts

When two devices edit the same file:

1. Both changes upload
2. Server detects conflict
3. Creates `conflict-2026-01-29-page.md`
4. **User must manually merge**

**Prevention**: Avoid editing same page on multiple devices simultaneously.

> **Removed:** This conflict flow applies to the old File Sync implementation and is not active in current source.

### RTC Conflict Resolution

When two users edit the same block:

1. Both operations sent to server
2. Server validates/appends tx batches, and clients pull/apply remote txs
3. Client applies remote txs and rebases pending local txs
4. Both clients receive merged result
5. **No user intervention needed**

When rebasing cannot safely replay a local pending operation, that pending tx can be dropped/marked non-pending, and conflict details are tracked for affected blocks.

---

## Performance Characteristics

### File Sync

| Metric                    | Value                   |
| ------------------------- | ----------------------- |
| Sync interval             | ~10 seconds             |
| Initial sync (1000 pages) | Minutes                 |
| Change detection          | File watching + polling |
| Network usage             | Full file on any change |

> **Removed:** These File Sync performance characteristics are historical-only.

### RTC

| Metric        | Value                 |
| ------------- | --------------------- |
| Sync latency  | <1 second             |
| Initial sync  | Seconds (delta only)  |
| Connection    | Persistent WebSocket  |
| Network usage | Operation deltas only |

---

## Migration Between Methods

### File Sync → RTC

1. Export Markdown graph
2. Create new database graph
3. Import content

**Note**: Some formatting may change during conversion.

> **Removed:** This migration path is historical guidance for older File Sync users.

### RTC → File Sync

1. Export database to Markdown
2. Create new file-based graph
3. Enable File Sync

**Note**: Typed properties become text properties.

> **Removed:** This migration path is historical; File Sync is not an active cloud sync path in current source.

---

## Security Comparison

| Aspect                | File Sync | RTC               |
| --------------------- | --------- | ----------------- |
| Encryption at rest    | ❌        | ✅ AES-256-GCM    |
| Encryption in transit | ✅ TLS    | ✅ TLS + E2EE     |
| Key management        | N/A       | User password     |
| Server can read data  | ⚠️ Yes    | ❌ Zero-knowledge |

RTC uses a hybrid encryption model:

- **RSA-4096** for key exchange
- **AES-256-GCM** for data encryption
- **PBKDF2** (600k iterations) for password key derivation

---

## Troubleshooting

### File Sync Issues

| Problem                  | Solution                                 |
| ------------------------ | ---------------------------------------- |
| Sync not starting        | Check login status, network              |
| Conflict files appearing | Merge manually, avoid simultaneous edits |
| Large files skipped      | Files >100MB won't sync                  |
| Slow initial sync        | Large graphs take time, be patient       |

> **Removed:** File Sync troubleshooting is historical-only.

### RTC Issues

| Problem          | Solution                                  |
| ---------------- | ----------------------------------------- |
| Not connecting   | Check WebSocket, firewall                 |
| Sync paused      | Network issue, will auto-resume           |
| Wrong password   | Cannot recover—password is encryption key |
| Version mismatch | Update all devices to same version        |

---

## Recommendations

| Scenario                  | Recommendation |
| ------------------------- | -------------- |
| Solo user, simple needs   | RTC            |
| Solo user, power features | RTC            |
| Team collaboration        | RTC            |
| Developer (Git workflow)  | RTC + export for Git workflows |
| Privacy-critical data     | RTC (E2EE)     |
| Maximum portability       | RTC + Markdown export/import |

---

## See Also

- **E2EE Security**: [e2ee-encryption.md](../security/e2ee-encryption.md)
- **RTC Setup Tutorial**: [real-time-sync.md](../../tutorials/collaboration/real-time-sync.md)
- **Database Graphs**: [file-vs-db.md](../concepts/file-vs-db.md)

---

_Last updated: 2026-05-30_
