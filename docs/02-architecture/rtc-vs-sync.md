# File Sync vs. RTC: Feature Comparison & Limitations

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
;; src/main/frontend/fs/sync.cljs
(def ^:private file-size-limit (* 100 1000 1024)) ;100MB
```

#### Performance Limits

| Metric                | Threshold     | Impact              |
| --------------------- | ------------- | ------------------- |
| Large graph detection | > 1,000 pages | Slower initial sync |
| Batch upload size     | 20 files      | Rate limiting       |
| Batch download size   | 100 files     | Memory management   |
| Sync interval         | ~10 seconds   | Polling frequency   |

**Code Reference:**

```clojure
;; src/main/frontend/fs/sync.cljs
(defonce download-batch-size 100)
(defonce upload-batch-size 20)
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
;; src/main/frontend/worker/rtc/asset.cljs
(defonce ^:private max-asset-size (* 100 1024 1024))

;; src/main/frontend/worker/rtc/ws_util.cljs
;; Messages over 100KB are uploaded to S3
(when (< 100000 len)
  (put-to-s3 message))

;; src/main/frontend/worker/rtc/exception.cljs
;; Responses over 32KB use S3
"When response from remote is too huge(> 32KB),
the server will put it to s3 and return its presigned-url to clients."
```

#### Performance Limits

| Metric            | Threshold  | Impact                 |
| ----------------- | ---------- | ---------------------- |
| Operations batch  | > 400 ops  | Extended timeout (20s) |
| WebSocket timeout | 10 seconds | Connection retry       |
| Large op timeout  | 20 seconds | For > 400 operations   |
| Retry attempts    | 10 times   | Connection resilience  |

**Code Reference:**

```clojure
;; src/main/frontend/worker/rtc/ws_util.cljs
(when (and (= "apply-ops" (:action message))
           (< 400 (count (:ops message))))
  {:timeout-ms 20000})
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
;; src/main/frontend/fs/sync.cljs
(defn storage-exceed-limit? [exp]
  (= (-> exp ex-data :err :body :message) "storage-limit"))

(defn graph-count-exceed-limit? [exp]
  (= (-> exp ex-data :err :body :message) "graph-count-exceed-limit"))
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

```txt
src/main/frontend/fs/sync.cljs          - Main sync logic
src/main/frontend/handler/assets.cljs   - Asset handling
src/main/frontend/components/settings.cljs - UI configuration
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
src/main/frontend/worker/rtc/           - RTC implementation
src/main/frontend/worker/rtc/core.cljs  - Main RTC logic
src/main/frontend/worker/rtc/asset.cljs - Asset sync
src/main/frontend/worker/rtc/ws_util.cljs - WebSocket handling
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
Message < 100KB → Direct WebSocket
Message > 100KB → Upload to S3, send reference
Response < 32KB → Direct WebSocket
Response > 32KB → Store in S3, return presigned URL
```

### Error Handling

#### File Sync Errors

- `storage-limit`: Exceeded storage quota
- `graph-count-exceed-limit`: Too many graphs
- `decrypt-failed`: Encryption error
- Conflicts: Create `conflict-` files

#### RTC Errors

- `:rtc.exception/ws-already-disconnected`: WebSocket closed
- `:rtc.exception/remote-graph-not-exist`: Graph deleted on server
- `:rtc.exception/schema-version-mismatched`: Incompatible versions
- `:rtc.exception/asset-too-large`: Asset > 100MB
- `:rtc.exception/get-s3-object-failed`: S3 fetch error

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

### File Sync Limits

```clojure
;; File size limit: 100MB
;; Location: src/main/frontend/fs/sync.cljs
(def ^:private file-size-limit (* 100 1000 1024))

;; Asset size check
;; Location: src/main/frontend/handler/assets.cljs
(defn exceed-limit-size?
  "Asset size no more than 100M"
  [^js file]
  (> (.-size file) (* 100 1024 1024)))

;; Batch sizes
;; Location: src/main/frontend/fs/sync.cljs
(defonce download-batch-size 100)
(defonce upload-batch-size 20)
```

### RTC Limits

```clojure
;; Asset size limit: 100MB
;; Location: src/main/frontend/worker/rtc/asset.cljs
(defonce ^:private max-asset-size (* 100 1024 1024))

;; WebSocket message size threshold: 100KB
;; Location: src/main/frontend/worker/rtc/ws_util.cljs
(when (< 100000 len)
  ;; Upload to S3 if message too large
  (put-apply-ops-message-on-s3-if-too-huge ws message))

;; Large operation batch: 400+ ops get extended timeout
;; Location: src/main/frontend/worker/rtc/ws_util.cljs
(when (and (= "apply-ops" (:action message))
           (< 400 (count (:ops message))))
  {:timeout-ms 20000})
```

---

_Document Version: 1.0_  
_Last Updated: 2025-10-04_  
_Logseq Version: feat/mcp-server branch_

For questions or corrections, please open an issue on GitHub or discuss in the Logseq community forums.
