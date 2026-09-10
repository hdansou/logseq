---
title: 'Local-First Architecture'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'Explanation'
platform: 'Desktop + Mobile'
related-docs:
  - '[File vs Database Graphs](./file-vs-database.md)'
  - '[E2EE Encryption](../security/e2ee-encryption.md)'
  - '[Database Integrity](../architecture/database-integrity.md)'
tags: ['architecture', 'local-first', 'privacy', 'offline', 'sync']
---

# Local-First Architecture

This document explains Logseq's local-first architecture philosophy. Understanding this approach helps you appreciate why Logseq works the way it does and how it protects your data and privacy.

**What You'll Understand:**

- What "local-first" means
- Why local storage matters
- How Logseq implements local-first
- Sync strategies and trade-offs
- Privacy and ownership implications
- Comparison with cloud-first apps

---

## Table of Contents

1. [Local-First Principles](#local-first-principles)
2. [Why Local-First?](#why-local-first)
3. [Logseq's Implementation](#logseqs-implementation)
4. [Storage Architectures](#storage-architectures)
5. [Sync Strategies](#sync-strategies)
6. [Privacy & Ownership](#privacy--ownership)
7. [Trade-offs](#trade-offs)
8. [Future of Local-First](#future-of-local-first)

---

## Local-First Principles

### Core Definition

**Local-First:** Software that stores and processes data primarily on the user's device, with optional cloud sync as a secondary feature.

**Key Principles:**

```markdown
1. No server required for core functionality
   ✅ Work offline by default
   ✅ Full features without internet
   ✅ No mandatory account

2. Data lives on your device
   ✅ You have the files
   ✅ Direct file access
   ✅ No proprietary formats (or open formats)

3. Fast and responsive
   ✅ No network latency
   ✅ Instant operations
   ✅ No loading spinners

4. Privacy by default
   ✅ Data never leaves device (unless you choose)
   ✅ No server-side tracking
   ✅ You control sharing

5. Longevity
   ✅ Works without company servers
   ✅ Data readable in 10+ years
   ✅ No vendor lock-in
```

### Local-First vs Cloud-First

**Cloud-First (Traditional SaaS):**

```markdown
Architecture:
┌─────────┐ Internet ┌─────────┐
│ Browser │ ←──────────────→ │ Server │
│ (View) │ Every action │ (Truth) │
└─────────┘ └─────────┘

Characteristics:

- Server is source of truth
- Requires internet for all operations
- Data stored on company servers
- Real-time collaboration easy
- Vendor controls data access

Examples: Notion, Google Docs, Evernote
```

**Local-First (Logseq):**

```markdown
Architecture:
┌─────────────┐ Optional ┌─────────┐
│ Local App │ ←────────→ │ Sync │
│ (Truth) │ Sync │ (Mirror)│
└─────────────┘ └─────────┘

Characteristics:

- Device is source of truth
- Works fully offline
- Data stored locally
- Collaboration requires sync strategy
- User controls data

Examples: Logseq, Obsidian (local), VS Code
```

---

## Why Local-First?

### 1. Speed & Responsiveness

**Local Operations Are Instant:**

```markdown
Cloud-first action:
User types → Send to server → Server processes →
Send back → Update UI
Time: 50-500ms (depending on connection)

Local-first action:
User types → Local process → Update UI
Time: 1-10ms (instant)

Result:

- No lag while typing
- No "saving..." spinners
- Instant search results
- Smooth interactions
```

**Performance Comparison:**

| Operation        | Cloud-First | Local-First |
| ---------------- | ----------- | ----------- |
| Load app         | 2-10s       | <1s         |
| Search 10k notes | 1-3s        | <100ms      |
| Create page      | 200-500ms   | <10ms       |
| Switch pages     | 100-300ms   | <10ms       |
| Edit block       | 50-200ms    | <5ms        |

### 2. Offline Functionality

**Work Anywhere:**

```markdown
Scenarios:
✈️ Airplane mode: Full functionality
🚆 Train with spotty WiFi: No interruptions
🏔️ Remote location: Works perfectly
🔌 Internet outage: Business as usual
🌐 Censored network: Complete access

Cloud-first apps:
❌ Login fails without internet
❌ "You're offline" error screens
❌ Lost work when connection drops
❌ Degraded or no functionality
```

### 3. Privacy & Control

**Your Data, Your Device:**

```markdown
Local-first:

- Data never leaves device (by default)
- No server logging your activity
- No data mining for ads
- No third-party access
- Government requests? No data to hand over

Cloud-first:

- All data on company servers
- Activity logs stored indefinitely
- Potential data mining
- Third-party integrations can access data
- Subject to subpoenas/requests
```

### 4. Longevity

**Future-Proof:**

```markdown
Local-first (Logseq):

- Files in standard formats (Markdown, SQLite)
- Readable without Logseq
- Works even if Logseq shuts down
- Data portable to other tools
- 10+ year accessibility guaranteed

Cloud-first:

- Proprietary formats
- Dependent on company servers
- If company closes, data at risk
- Export may be incomplete
- Lock-in risk
```

**Historical Examples:**

```markdown
Services that shut down:

- Google Reader (2013)
- Sunrise Calendar (2016)
- Inbox by Gmail (2019)
- Evernote (struggling)

Local-first never "shuts down"
Your files remain accessible
```

### 5. Cost Structure

**Economic Benefits:**

```markdown
Cloud-first recurring costs:

- $10-15/month/user forever
- Costs increase with usage
- Features behind paywall
- Price increases over time

Local-first one-time cost:

- Free (open source like Logseq)
- Or one-time purchase
- All features available
- No recurring fees
- Optional sync subscription
```

---

## Logseq's Implementation

### Two Graph Types

**File-Based Graphs:**

```markdown
Storage: Plain Markdown files

Structure:
LogseqGraph/
├── journals/
│ ├── 2026-01-22.md
│ └── 2026-01-21.md
├── pages/
│ ├── Projects\_\_\_Alpha.md
│ └── Meeting Notes.md
├── assets/
│ └── images/
└── logseq/
├── config.edn
└── custom.css

Benefits:
✅ Human-readable
✅ Git-friendly
✅ Direct file editing
✅ Easy backup
✅ Tool-agnostic
```

**Database Graphs:**

```markdown
Storage: SQLite database

Structure:
LogseqGraph/
├── logseq.db (SQLite file)
├── version-files/ (Transit exports)
│ └── graph-1737504000.transit
└── assets/

Benefits:
✅ Faster queries
✅ Better performance (large graphs)
✅ Advanced features
✅ Schema validation
✅ RTC sync support
```

### Local Storage Technologies

**Desktop (Electron):**

```javascript
// File system access
const fs = require('fs')

// Read file
fs.readFileSync('/path/to/file.md', 'utf-8')

// Write file
fs.writeFileSync('/path/to/file.md', content)

// Direct file system access
// No server roundtrip
```

**Mobile (Capacitor):**

```typescript
// Filesystem plugin
import { Filesystem } from '@capacitor/filesystem'

// Read
await Filesystem.readFile({
  path: 'file.md',
  directory: Directory.Documents,
})

// Write
await Filesystem.writeFile({
  path: 'file.md',
  data: content,
  directory: Directory.Documents,
})
```

**Browser (Web):**

```javascript
// File System Access API (modern browsers)
const handle = await window.showDirectoryPicker()

// Or IndexedDB for database graphs
const db = await openDB('logseq', 1)
```

### In-Memory Processing

**DataScript (File-Based):**

```clojure
;; Load files into memory
(def conn (d/create-conn schema))

;; Parse markdown → blocks
(def blocks (parse-files files))

;; Transact to in-memory DB
(d/transact! conn blocks)

;; Query in-memory
(d/q '[:find ?title
       :where [?p :block/name ?title]]
     @conn)

;; No server queries
;; All local, all fast
```

**SQLite (Database):**

```sql
-- Query local SQLite
SELECT * FROM blocks
WHERE marker = 'TODO'
AND deadline < date('now')

-- Runs on device
-- No network latency
-- Privacy preserved
```

---

## Storage Architectures

### File-Based Architecture

**How It Works:**

```markdown
1. Read Phase:
   - Scan directory for .md files
   - Parse markdown to blocks
   - Build graph in memory (DataScript)
   - Extract properties, links, refs
2. Work Phase:
   - All operations in-memory
   - Instant queries
   - Real-time updates
3. Write Phase:
   - Detect changes
   - Write modified files
   - Update timestamps
   - Preserve formatting
```

**File Structure Example:**

```markdown
File: pages/Projects\_\_\_Alpha.md

Content:

- [[Project Alpha]]
  status:: active
  owner:: [[Person/Sarah]]

  - TODO Complete milestone 1
    DEADLINE: <2026-01-30 Wed>

Parsed to:
Block 1: {uuid: abc-123, content: "Project Alpha", ...}
Block 2: {uuid: def-456, parent: abc-123, ...}
```

### Database Architecture

**How It Works:**

```markdown
1. Open Phase:
   - Open SQLite file
   - Load schema
   - Prepare indexes
2. Work Phase:
   - SQL queries for data
   - Transactions for writes
   - ACID guarantees
3. Sync Phase:
   - Export transit file
   - Upload to sync service
   - Resolve conflicts
```

**Schema Structure:**

```sql
-- Simplified schema
CREATE TABLE blocks (
  id INTEGER PRIMARY KEY,
  uuid TEXT UNIQUE,
  content TEXT,
  parent_id INTEGER,
  page_id INTEGER,
  properties JSON,
  created_at INTEGER,
  updated_at INTEGER
);

CREATE INDEX idx_blocks_uuid ON blocks(uuid);
CREATE INDEX idx_blocks_page ON blocks(page_id);
```

---

## Sync Strategies

### Git Sync (File-Based)

**How It Works:**

```bash
# Your workflow
git add .
git commit -m "Daily updates"
git push origin main

# On another device
git pull origin main

# Automatic with scripts
# Run on file changes
```

**Benefits:**

```markdown
✅ Version control
✅ Branching/merging
✅ Full history
✅ Conflict resolution
✅ Free (GitHub/GitLab)
✅ Works offline (commit locally)
```

**Challenges:**

```markdown
⚠️ Manual process (or scripts)
⚠️ Merge conflicts require resolution
⚠️ Large binary assets problematic
⚠️ Learning curve for Git
```

### iCloud/Dropbox (File-Based)

**How It Works:**

```markdown
1. Save graph in synced folder
   ~/Dropbox/LogseqGraph/
2. File changes sync automatically
3. Access from multiple devices
4. Conflict files created on collision
```

**Benefits:**

```markdown
✅ Automatic sync
✅ Easy setup
✅ Works with existing accounts
✅ No Git knowledge needed
```

**Challenges:**

```markdown
⚠️ Conflict files (.conflicted)
⚠️ No version history
⚠️ Sync timing issues
⚠️ No encryption in transit
```

### Logseq Sync (Database)

**How It Works:**

```markdown
1. Local changes → Transit export
2. Encrypt with E2EE
3. Upload to Logseq servers
4. Download on other devices
5. Decrypt locally
6. Merge changes (CRDT)

Server never sees unencrypted data
```

**Benefits:**

```markdown
✅ Real-time sync
✅ End-to-end encryption
✅ Conflict-free (CRDT)
✅ Mobile support
✅ Integrated experience
```

**Challenges:**

```markdown
⚠️ Requires subscription ($5/mo)
⚠️ Database graphs only
⚠️ Dependent on Logseq service
```

### Self-Hosted Sync

**Options:**

```markdown
1. Syncthing (file-based)
   - P2P sync
   - No central server
   - Free and open source
2. rsync (file-based)

   - Your own server
   - Full control
   - Cron automation

3. CRDTs (database)
   - Self-host sync server
   - Advanced setup
   - Full privacy
```

---

## Privacy & Ownership

### Data Ownership

**You Own Your Data:**

```markdown
File-based:

- Plain markdown files
- In your filesystem
- Backup with any tool
- Read with any editor

Database:

- SQLite file you control
- Transit exports (readable)
- Can extract all data
- Portable format

Contrast with cloud-first:

- Data on their servers
- Proprietary formats
- Limited export options
- Dependent on their API
```

### Privacy Guarantees

**What Logseq Knows (or Doesn't):**

```markdown
Local operation:

- Logseq app: Knows nothing (no telemetry)
- Your device: All data
- Internet: No data sent

With Logseq Sync:

- Logseq servers: Encrypted blobs only
- No unencrypted content
- No usage tracking
- No data mining

With Git/Dropbox:

- Git host: Sees file contents (use private repo)
- Dropbox: Sees file contents
- No analytics by Logseq
```

### Data Portability

**Exit Strategy:**

```markdown
Logseq → Other Tools:

File-based:

1. Already in Markdown
2. Import to Obsidian, Notion, any editor
3. Zero conversion needed

Database:

1. Export to Markdown
2. Export to JSON/EDN
3. Parse with custom scripts
4. No lock-in

Timeline: Minutes to hours
Data loss: Minimal to none
Vendor lock-in: None
```

---

## Trade-offs

### Advantages

**✅ Speed & Performance**

```markdown
- Instant operations
- No loading states
- Smooth experience
- Large graphs performant
```

**✅ Privacy & Security**

```markdown
- Data on your device
- No tracking
- No data breaches
- You control access
```

**✅ Offline Capability**

```markdown
- Work anywhere
- No internet required
- Reliable access
- No downtime
```

**✅ Ownership & Longevity**

```markdown
- You own files
- Standard formats
- Future-proof
- No vendor lock-in
```

### Disadvantages

**❌ Collaboration Complexity**

```markdown
Challenge: Real-time collaboration harder

- No Google Docs-style live editing
- Sync conflicts possible
- Requires sync strategy setup

Mitigation:

- Logseq Sync for teams
- Git workflows
- Single-editor-at-a-time
```

**❌ Multi-Device Sync Setup**

```markdown
Challenge: Requires configuration

- Git: Manual setup
- Dropbox: Potential conflicts
- Logseq Sync: Paid feature

Cloud-first:

- Just log in
- Automatic sync
- No setup

Trade-off: Convenience vs Privacy
```

**❌ Mobile Limitations**

```markdown
Challenge: Mobile filesystem access

- iOS: Restricted file access
- Android: Better but still limited
- Background sync: Battery drain

Database graphs help, but:

- Different architecture needed
- More complex sync
```

**❌ Backup Responsibility**

```markdown
Challenge: You must backup

- Cloud-first: Automatic backups
- Local-first: Your responsibility

Solutions:

- Git push (automatic backup)
- Cloud storage (Dropbox/iCloud)
- Scheduled local backups
- Multiple copies
```

---

## Future of Local-First

### Emerging Technologies

**CRDTs (Conflict-Free Replicated Data Types):**

```markdown
What: Data structures that merge automatically
Why: Enable true real-time collaboration
Status: Logseq RTC uses CRDTs

Benefits:

- No conflict resolution needed
- Real-time editing
- Offline-first still works
- Best of both worlds
```

**WebAssembly:**

```markdown
What: Run native code in browser
Why: Full app in browser with local storage
Status: Possible future for Logseq

Benefits:

- No install needed
- Full performance
- Access anywhere
- Still local-first
```

**Peer-to-Peer Sync:**

```markdown
What: Direct device-to-device sync
Why: No central server needed
Status: Experimental (Syncthing model)

Benefits:

- Maximum privacy
- No subscription
- Fast local network sync
- Resilient
```

### Industry Trends

**More Apps Going Local-First:**

```markdown
2020s trend:

- Obsidian (local Markdown)
- Logseq (local-first)
- Linear (local cache)
- Figma (local state)
- VS Code (local files)

Why now:

- Privacy concerns growing
- Fast devices
- Better web APIs
- User demand for ownership
```

---

## Comparison Table

| Aspect            | Local-First (Logseq)       | Cloud-First (Notion) |
| ----------------- | -------------------------- | -------------------- |
| **Speed**         | Instant                    | 50-500ms lag         |
| **Offline**       | Full functionality         | Limited/none         |
| **Privacy**       | Data on device             | Data on servers      |
| **Ownership**     | You own files              | Company owns data    |
| **Longevity**     | Forever (standard formats) | Dependent on company |
| **Collaboration** | Requires sync setup        | Built-in, easy       |
| **Setup**         | Manual sync config         | Just login           |
| **Cost**          | Free (optional sync)       | $8-10/mo             |
| **Backup**        | Your responsibility        | Automatic            |
| **Portability**   | Easy (Markdown)            | Export required      |

---

## Next Steps

**Embrace Local-First:**

1. Understand your data lives locally
2. Set up backup strategy
3. Configure sync (Git/Cloud/Logseq)
4. Enjoy privacy and speed
5. Share benefits with others

**Related:**

- [File vs Database Graphs](./file-vs-database.md)
- [E2EE Encryption](../security/e2ee-encryption.md)
- [Backup Strategies](../../how-to/organization/backup-strategies.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
