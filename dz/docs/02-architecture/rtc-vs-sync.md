# File Sync vs. RTC: Feature Comparison & Limitations

> **Last revised:** 2026-05-28

## Table of Contents

- [Overview](#overview)
- [Architecture Comparison](#architecture-comparison)
- [Feature Matrix](#feature-matrix)
- [Limitations & Constraints](#limitations--constraints)
- [Performance Characteristics](#performance-characteristics)
- [Use Cases & Recommendations](#use-cases--recommendations)
- [Migration Considerations](#migration-considerations)
- [Technical Details](#technical-details)

## Overview

Logseq offers two synchronization methods for keeping your knowledge graph in sync across devices and enabling collaboration:

1. **File Sync** (Markdown-based): Traditional file-based synchronization for `.md` files
2. **RTC** (Real-Time Collaboration): Database-level synchronization for SQLite-based graphs with live collaboration

This document provides a comprehensive comparison of both approaches, their limitations, and guidance on choosing the right one for your needs.

> **Removed:** File Sync (Markdown-based cloud sync) has been removed from the codebase. The old implementation (`src/main/frontend/fs/sync.cljs`) no longer exists, and the `feature-matrix {:file-sync :beta}` entry is commented out in `src/main/frontend/handler/user.cljs`. All sections in this document that describe File Sync behavior, limits, and implementation reflect historical versions only. The `/file-sync/` REST API endpoint name is retained solely for quota-management calls (storage limit, graph count) that now serve the RTC/DB sync feature.

## Architecture Comparison

### File Sync (Markdown)

```txt
┌─────────────┐         File Changes        ┌─────────────┐
│   Device A  │────────────────────────────►│   Cloud     │
│  (.md files)│◄────────────────────────────│  Storage    │
└─────────────┘                             └─────────────┘
                                                    ▲
                                                    │
                                              File Changes
                                                    │
                                                    ▼
                                             ┌─────────────┐
                                             │   Device B  │
                                             │  (.md files)│
                                             └─────────────┘
```

**Key Characteristics:**

- File-level synchronization
- Conflict resolution at file level
- Works with any text editor
- Supports Markdown graphs only
- Periodic sync (not real-time)

### RTC (Real-Time Collaboration)

```txt
┌─────────────┐      Operations/Deltas      ┌─────────────┐
│   Device A  │────────────────────────────►│   RTC       │
│  (SQLite DB)│◄────────────────────────────│  Server     │
└─────────────┘      WebSocket/HTTP         └─────────────┘
                                                    ▲
                                                    │
                                             Operations/Deltas
                                                    │
                                                    ▼
                                             ┌─────────────┐
                                             │   Device B  │
                                             │  (SQLite DB)│
                                             └─────────────┘
```

**Key Characteristics:**

- Operation-level synchronization (CRDT-like)
- Real-time updates via WebSocket
- Automatic conflict resolution
- Database graphs only
- Live collaboration support

## Feature Matrix

| Feature                 | File Sync (MD) | RTC (DB)      | Notes                              |
| ----------------------- | -------------- | ------------- | ---------------------------------- |
| **Graph Type**          | Markdown only  | Database only | Cannot mix                         |
| **Real-time sync**      | ❌ No          | ✅ Yes        | RTC: ~instant, File Sync: periodic |
| **Collaboration**       | ⚠️ Limited     | ✅ Full       | File Sync has conflict issues      |
| **Offline support**     | ✅ Yes         | ✅ Yes        | Both support offline work          |
| **External editors**    | ✅ Yes         | ❌ No         | File Sync works with any editor    |
| **Version history**     | ✅ Yes         | ✅ Yes        | Both support versioning            |
| **Asset sync**          | ✅ Yes         | ✅ Yes        | Both support images, PDFs, etc.    |
| **Cross-platform**      | ✅ Yes         | ✅ Yes        | All major platforms supported      |
| **Multi-user editing**  | ❌ Poor        | ✅ Excellent  | RTC designed for collaboration     |
| **Conflict resolution** | ⚠️ Manual      | ✅ Automatic  | RTC uses operational transforms    |
| **Bandwidth usage**     | Higher         | Lower         | RTC syncs operations, not files    |
| **Setup complexity**    | Simple         | Simple        | Both easy via settings             |
| **Migration**           | ✅ Easy export | ⚠️ Limited    | Easier to leave File Sync          |

## Limitations & Constraints

### File Sync Limitations

#### Size Limits

| Resource              | Limit              | Technical Reason                          |
| --------------------- | ------------------ | ----------------------------------------- |
| **Per-file size**     | **100 MB**         | Network transfer and storage optimization |
| **Total graph size**  | Subscription-based | Cloud storage quota                       |
| **Number of graphs**  | Subscription-based | Service tier limits                       |
| **Individual assets** | **100 MB**         | Same as per-file limit                    |

**Code Reference:**

```clojure
;; src/main/frontend/handler/assets.cljs
(defn exceed-limit-size?
  "Asset size no more than 100M"
  [^js file]
  (> (.-size file) (* 100 1024 1024)))
```

#### Performance Limits

| Metric                | Threshold     | Impact              |
| --------------------- | ------------- | ------------------- |
| Large graph detection | > 1,000 pages | Slower initial sync |
| Batch upload size     | 20 files      | Rate limiting       |
| Batch download size   | 100 files     | Memory management   |
| Sync interval         | ~10 seconds   | Polling frequency   |

> **Note:** The specific thresholds in this table (1 000 pages, 20 files, 100 files, 10 s interval) could not be confirmed in current source code; they reflect historical documentation and should be treated as approximate.

**Code Reference:**

```clojure
;; src/main/frontend/worker/sync/upload.cljs
(def upload-kvs-batch-size 500)
```

#### Operational Limits

- **Conflict resolution**: Manual - users must resolve conflicting changes
- **Concurrent editors**: Not recommended - high conflict probability
- **Real-time updates**: No - periodic polling only
- **Asset sync**: Sequential - not parallelized for large numbers

#### Known Issues

1. **Conflict Files**: When multiple devices edit simultaneously, `conflict-` files are created
2. **Sync Delays**: Changes may take 10-60 seconds to appear on other devices
3. **Large File Skipping**: Files over 100MB are silently skipped with notification
4. **Network Sensitivity**: Poor connectivity can cause sync failures

### RTC Limitations

#### Size Limits

| Resource              | Limit                   | Technical Reason                 |
| --------------------- | ----------------------- | -------------------------------- |
| **Per-asset size**    | **100 MB**              | S3 upload limits and performance |
| **WebSocket message** | **100 KB** (auto-split) | Protocol efficiency              |
| **Server response**   | **32 KB** (S3 fallback) | WebSocket frame limits           |
| **Total graph size**  | No hard limit\*         | Uses incremental sync            |
| **Number of graphs**  | Subscription-based      | Service tier limits              |

\* _Theoretical limit exists but not enforced in client code_

**Code References:**

```clojure
;; src/main/frontend/worker/sync/assets.cljs
(def max-asset-size (* 100 1024 1024))
```

> **Note:** The RTC implementation is organized across multiple sub-modules in `src/main/frontend/worker/sync/`. The parent `sync.cljs` orchestrates connections and reconnect logic; individual concerns are separated into `upload.cljs`, `download.cljs`, `assets.cljs`, `transport.cljs`, `large_title.cljs`, `handle_message.cljs`, and others.

#### Performance Limits

| Metric                    | Threshold              | Impact                            |
| ------------------------- | ---------------------- | --------------------------------- |
| WS stale kill timeout     | 600,000 ms (10 min)    | Stale WebSocket forcibly killed   |
| Reconnect base delay      | 1,000 ms (1 s)         | Exponential backoff start         |
| Reconnect max delay       | 30,000 ms (30 s)       | Backoff ceiling                   |
| Reconnect jitter          | 250 ms                 | Spread reconnect storms           |
| Retry attempts            | Unlimited              | Reconnects until manually stopped |
| Large-title offload limit | 4,096 bytes (4 KB)     | Block titles > 4 KB go to S3      |
| Snapshot batch size       | 1,000,000 bytes (1 MB) | Snapshot upload chunks            |

> **Note:** The previously documented thresholds of "Operations batch > 400 ops → 20 s timeout", "WebSocket timeout 10 s", and "10 retry attempts" could not be confirmed in current source code. The values above are derived from `src/main/frontend/worker/sync.cljs` and sub-modules.

**Code Reference:**

```clojure
;; src/main/frontend/worker/sync.cljs
(def ^:private reconnect-base-delay-ms 1000)
(def ^:private reconnect-max-delay-ms   30000)
(def ^:private reconnect-jitter-ms      250)
(def ^:private ws-stale-timeout-ms      600000)

;; src/main/frontend/worker/sync/upload.cljs
(def upload-kvs-batch-size 500)
(def snapshot-upload-max-bytes 1000000)

;; src/main/frontend/worker/sync/large_title.cljs
(def large-title-byte-limit 4096)
```

#### Operational Limits

- **Concurrent users**: Unlimited (server-side limit may apply)
- **Operation history**: Full history maintained
- **Schema versions**: Must match across clients
- **Network dependency**: Requires persistent connection for real-time features

#### Known Constraints

1. **Asset Size**: Assets over 100MB are logged as "too large" and skipped
2. **Schema Compatibility**: All clients must use compatible schema versions
3. **WebSocket Required**: Real-time features need stable WebSocket connection
4. **Database Only**: Only works with DB-based graphs (not Markdown)

### Subscription-Based Limits

These limits are enforced by the Logseq cloud service based on your subscription tier:

| Limit Type          | Free Tier | Pro/Team Tier | Notes                    |
| ------------------- | --------- | ------------- | ------------------------ |
| **Storage quota**   | Limited   | Higher        | Total cloud storage      |
| **Graph count**     | Limited   | Higher        | Number of synced graphs  |
| **Collaboration**   | Limited   | Full          | Number of collaborators  |
| **Version history** | Basic     | Extended      | History retention period |

**Code Reference:**

```clojure
;; src/main/frontend/handler/user.cljs
(defn storage-exceed-limit? [exp]
  (some->> (ex-data exp) :err
           ((juxt :status (comp :message :body)))
           ((fn [[status msg]] (and (= 403 status) (= msg "storage-limit"))))))

(defn graph-count-exceed-limit? [exp]
  (some->> (ex-data exp) :err
           ((juxt :status (comp :message :body)))
           ((fn [[status msg]] (and (= 403 status) (= msg "graph-count-exceed-limit"))))))
```

## Performance Characteristics

### Sync Speed

| Scenario                  | File Sync | RTC      | Winner    |
| ------------------------- | --------- | -------- | --------- |
| Small change (1 block)    | ~10-30s   | < 1s     | 🏆 RTC    |
| Large change (100 blocks) | ~30-60s   | ~2-5s    | 🏆 RTC    |
| New file/page             | ~10-30s   | < 1s     | 🏆 RTC    |
| Asset upload              | ~5-60s    | ~5-30s   | ≈ Similar |
| Initial sync (1000 pages) | ~5-15 min | ~2-5 min | 🏆 RTC    |

### Bandwidth Usage

| Operation           | File Sync                | RTC                  | Notes                      |
| ------------------- | ------------------------ | -------------------- | -------------------------- |
| Small text edit     | Full file                | Operations only      | RTC: ~10-100x less data    |
| Polling overhead    | Periodic metadata checks | WebSocket keepalive  | RTC: persistent connection |
| Large asset         | Full upload/download     | Full upload/download | Similar for assets         |
| Conflict resolution | Full file re-upload      | Automatic merge      | RTC more efficient         |

### Memory Usage

| Aspect         | File Sync    | RTC             | Notes                            |
| -------------- | ------------ | --------------- | -------------------------------- |
| Graph loading  | File-by-file | Database query  | RTC faster for large graphs      |
| Sync overhead  | File buffers | Operation queue | Similar memory footprint         |
| Asset handling | Sequential   | Concurrent      | RTC can be more memory-intensive |

### Network Resilience

| Scenario              | File Sync         | RTC                | Notes                          |
| --------------------- | ----------------- | ------------------ | ------------------------------ |
| Offline editing       | ✅ Full support   | ✅ Full support    | Both work offline              |
| Poor connection       | ⚠️ Sync delays    | ⚠️ WebSocket drops | File Sync may be more tolerant |
| Connection recovery   | Auto-retry        | Auto-reconnect     | Both handle well               |
| Conflict on reconnect | Manual resolution | Auto-merge         | RTC superior                   |

## Use Cases & Recommendations

### Choose File Sync (Markdown) When:

✅ **Best for:**

- You want to use external text editors (Vim, VS Code, etc.)
- You need Git integration or version control
- You're working solo or with minimal collaboration
- You want maximum portability (Markdown files)
- You need to use existing Markdown-based workflows
- You want to easily export/backup your data

❌ **Avoid when:**

- You need real-time collaboration
- Multiple people edit the same graph simultaneously
- You want instant sync across devices
- You need automatic conflict resolution

### Choose RTC (Database) When:

✅ **Best for:**

- Real-time collaboration with multiple users
- Instant sync across your devices
- Working with large graphs (faster sync)
- You primarily use Logseq app (not external editors)
- You need automatic conflict resolution
- You want to see others' changes live

❌ **Avoid when:**

- You want to edit with external text editors
- You need Git-based version control
- You require maximum data portability
- You're uncertain about committing to Logseq ecosystem

### Migration Scenarios

#### Markdown → Database

**When to migrate:**

- Starting to collaborate with team members
- Finding conflicts frustrating
- Want faster sync performance
- Growing graph size causing slow syncs

**How to migrate:**

1. Export Markdown graph
2. Create new DB graph
3. Import Markdown files
4. Enable RTC sync
5. ⚠️ **Note**: One-way migration - harder to go back

#### Database → Markdown

**When to migrate:**

- Want to use external editors
- Need better data portability
- Prefer file-based workflows
- Want simpler backup strategy

**How to migrate:**

1. Export DB graph to Markdown
2. Create new Markdown graph
3. Import exported files
4. Enable File Sync
5. ⚠️ **Note**: May lose some DB-specific features

## Technical Details

### File Sync Implementation

**Transport:** HTTP REST API
**Protocol:** File-based synchronization
**Storage:** Cloud object storage (S3-like)
**Sync Method:** Polling + push

**Key Components:**

> **Removed:** File Sync (Markdown-based cloud sync) has no implementation in the current codebase. The component list below reflects historical descriptions only.

```txt
src/main/frontend/handler/assets.cljs   - Asset handling (exceed-limit-size?)
src/main/frontend/handler/user.cljs     - Auth, storage limits, quota checks
```

**Sync Algorithm:**

1. Detect local changes (filesystem watch)
2. Poll server for remote changes (~10s interval)
3. Download changed files
4. Upload local changes
5. Handle conflicts (create conflict files)
6. Update metadata and checksums

### RTC Implementation

**Transport:** WebSocket (primary) + HTTP (fallback)
**Protocol:** Operational transformation (CRDT-inspired)
**Storage:** Server-side database + S3 for large payloads
**Sync Method:** Real-time push via WebSocket

**Key Components:**

```txt
src/main/frontend/worker/sync.cljs                   - Entry point: WS lifecycle, reconnect, orchestration
src/main/frontend/worker/sync/upload.cljs            - Snapshot upload batching (upload-kvs-batch-size, snapshot-upload-max-bytes)
src/main/frontend/worker/sync/download.cljs          - Snapshot download and import
src/main/frontend/worker/sync/assets.cljs            - Asset upload/download (max-asset-size)
src/main/frontend/worker/sync/transport.cljs         - WS message shaping and coercion
src/main/frontend/worker/sync/large_title.cljs       - Large-title offload to S3 (large-title-byte-limit)
src/main/frontend/worker/sync/apply_txs.cljs         - Apply remote transactions and conflict resolution
src/main/frontend/worker/sync/handle_message.cljs    - Incoming WS message dispatch
src/main/frontend/worker/sync/log_and_state.cljs     - RTC log types and state broadcasting
src/main/frontend/worker/sync/crypt.cljs             - E2EE key management and encryption
src/main/frontend/worker/sync/client_op.cljs         - Client-side pending operation tracking
src/main/frontend/worker/sync/presence.cljs          - Online-user presence tracking
src/main/frontend/worker/sync/auth.cljs              - WS token resolution
src/main/frontend/handler/events/rtc_error.cljs      - RTC error classification (download-decrypt-failed?)
```

**Sync Algorithm:**

1. Capture local operations (database transactions)
2. Push operations to server via WebSocket
3. Receive remote operations in real-time
4. Apply operations to local database
5. Resolve conflicts automatically (OT)
6. Maintain operation history

### Message Size Handling

#### File Sync

```txt
File < 100MB → Direct upload/download
File > 100MB → Skip with notification
```

#### RTC

```txt
Block title ≤ 4 KB  → Included inline in transaction
Block title > 4 KB  → Offloaded to S3 (large-title-byte-limit = 4096 bytes)

Snapshot chunk ≤ 1 MB → Direct HTTP upload
Snapshot chunk > 1 MB → Split into multiple batches (snapshot-upload-max-bytes = 1 000 000 bytes)
```

> **Removed:** The previously documented "100 KB WebSocket auto-split" and "32 KB S3 fallback" thresholds were not found in current source code. The actual large-message strategy is large-title offloading (4 KB threshold) and snapshot chunking (1 MB threshold).

### Error Handling

#### File Sync Errors

> **Removed:** File Sync (Markdown-based cloud sync) has been removed from the codebase. The error types below reflect the historical implementation.

- `storage-limit`: Exceeded storage quota (still enforced server-side for RTC graphs via `storage-exceed-limit?`)
- `graph-count-exceed-limit`: Too many graphs (still enforced server-side for RTC graphs via `graph-count-exceed-limit?`)
- `decrypt-failed`: Encryption error (historical; RTC now checks for "decrypt-aes-key" / "decrypt-private-key" text)
- Conflicts: Create `conflict-` files (file-level conflict files no longer produced; RTC uses block-level `sync-conflict-attrs`)

#### RTC Errors

- `:rtc.asset.log/asset-too-large`: Asset > 100 MB (defined in `log_and_state.cljs`)
- WebSocket disconnection → exponential-backoff reconnect (base 1 s, max 30 s, jitter 250 ms, unlimited retries)
- Schema version mismatch detection
- S3 object fetch failures
- Decrypt errors: checked via `download-decrypt-failed?` in `src/main/frontend/handler/events/rtc_error.cljs`, which looks for "decrypt-aes-key" or "decrypt-private-key" in the error chain

> **Removed:** The generic `decrypt-failed` error code listed in earlier versions of this document was not found in current source code. The actual check inspects error message text for the strings above.

## Findings: New Capabilities

The following capabilities were found in the current source code but were not mentioned in the original document:

### Large-Title Offloading

Block titles that exceed 4,096 bytes (UTF-8) are automatically offloaded to S3 and replaced with a reference object during RTC sync, then re-fetched on download. This is invisible to the user and prevents oversized WebSocket frames without an arbitrary 100 KB limit.

- **Threshold:** `large-title-byte-limit 4096` — `src/main/frontend/worker/sync/large_title.cljs`
- **Attribute:** `:logseq.property.sync/large-title-object`

### Snapshot Chunking

Database snapshots are split into 1 MB chunks before upload, so very large graphs can be synced without hitting HTTP body limits.

- **Limit:** `snapshot-upload-max-bytes 1000000` — `src/main/frontend/worker/sync/upload.cljs`

### Exponential-Backoff Reconnect

WebSocket reconnections use exponential backoff (not a fixed 10-attempt limit):

- **Base delay:** 1,000 ms · **Max delay:** 30,000 ms · **Jitter:** 250 ms
- **Max attempts:** unlimited
- **Stale-kill timeout:** 600,000 ms (10 min) — idle WS connections are forcibly terminated and replaced.

### Real-Time Presence

The `src/main/frontend/worker/sync/presence.cljs` module broadcasts which block each online user is currently editing. This is separate from the data-sync pipeline and has no equivalent in File Sync.

### Block-Level Conflict Resolution (RTC)

Rather than creating `conflict-` files, the RTC sync engine tracks conflicting block attributes in `sync-conflict-attrs` (`src/main/frontend/worker/sync/apply_txs.cljs`). Conflicts are surfaced as inline block annotations in the UI (`ls-sync-conflict-warning`).

---

## Summary Table

| Aspect                  | File Sync (MD)             | RTC (DB)               |
| ----------------------- | -------------------------- | ---------------------- |
| **Sync Speed**          | Seconds to minutes         | Sub-second to seconds  |
| **Collaboration**       | Poor (conflicts)           | Excellent (real-time)  |
| **Data Format**         | Markdown files             | SQLite database        |
| **Portability**         | High                       | Medium                 |
| **External Editors**    | ✅ Yes                     | ❌ No                  |
| **Real-time Updates**   | ❌ No                      | ✅ Yes                 |
| **Conflict Resolution** | Manual                     | Automatic              |
| **Large Graphs**        | Slower                     | Faster                 |
| **Bandwidth**           | Higher                     | Lower                  |
| **Setup**               | Simple                     | Simple                 |
| **Asset Limit**         | 100MB                      | 100MB                  |
| **Graph Size Limit**    | Subscription               | Subscription           |
| **Best For**            | Solo users, external tools | Teams, real-time needs |

## Recommendations

### For Individual Users

- **Small to medium graphs (< 500 pages)**: Either works well
- **Large graphs (> 1000 pages)**: RTC for better performance
- **Use external editors**: Must use File Sync
- **Multiple devices**: RTC for instant sync

### For Teams

- **Any team size**: RTC strongly recommended
- **Concurrent editing**: RTC required
- **Shared knowledge base**: RTC for live updates
- **File-based workflows**: File Sync if coordination is good

### For Organizations

- **Enterprise deployments**: RTC for control
- **Mixed workflows**: May need both (separate graphs)
- **Compliance needs**: Evaluate data storage options
- **Integration requirements**: File Sync for external tools

## Future Considerations

Both synchronization methods are actively developed. Future improvements may include:

**File Sync:**

- Better conflict resolution
- Faster sync algorithms
- Improved large file handling

**RTC:**

- Enhanced offline capabilities
- Better asset optimization
- Improved schema migration tools

---

## Appendix: Code References

### Sync Limits

```clojure
;; Asset size check
;; Location: src/main/frontend/handler/assets.cljs
(defn exceed-limit-size?
  "Asset size no more than 100M"
  [^js file]
  (> (.-size file) (* 100 1024 1024)))

;; Upload KV batch size
;; Location: src/main/frontend/worker/sync/upload.cljs
(def upload-kvs-batch-size 500)

;; Max snapshot upload bytes per batch
;; Location: src/main/frontend/worker/sync/upload.cljs
(def snapshot-upload-max-bytes 1000000)

;; Max asset size
;; Location: src/main/frontend/worker/sync/assets.cljs
(def max-asset-size (* 100 1024 1024))

;; Large-title offload threshold
;; Location: src/main/frontend/worker/sync/large_title.cljs
(def large-title-byte-limit 4096)

;; WS reconnect parameters
;; Location: src/main/frontend/worker/sync.cljs
(def ^:private reconnect-base-delay-ms 1000)
(def ^:private reconnect-max-delay-ms   30000)
(def ^:private reconnect-jitter-ms      250)
(def ^:private ws-stale-timeout-ms      600000)
```

### RTC/Asset Limits

```clojure
;; Asset size limit: 100MB
;; Location: src/main/frontend/worker/sync.cljs
(def ^:private max-asset-size (* 100 1024 1024))

;; Storage and graph count limits
;; Location: src/main/frontend/handler/user.cljs
;; Enforced server-side, detected via 403 status codes
```

---

_Document Version: 1.1_
_Last Updated: 2026-03-10_

For questions or corrections, please open an issue on GitHub or discuss in the Logseq community forums.
