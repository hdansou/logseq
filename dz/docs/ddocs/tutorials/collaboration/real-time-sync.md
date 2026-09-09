---
title: 'Getting Started with Real-Time Collaboration'
status: '🟡 Active Development'
version: 'Since v0.9.0'
quadrant: 'Tutorial'
platform: 'Desktop (macOS/Windows/Linux)'
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: merged-not-fully-audited
related-docs:
  - '../../explanation/security/e2ee-encryption.md'
  - '../../reference/features/user-profiles.md'
  - '../../how-to/collaboration/sharing-graphs.md'
---

# Getting Started with Real-Time Collaboration

> **Last revised:** 2026-09-09

## What You'll Learn

In this tutorial, you'll learn how to:

1. Enable Real-Time Collaboration (RTC) for your graph
2. Invite collaborators to work with you
3. See others' changes in real-time
4. Understand user presence and editing indicators
5. Resolve conflicts when editing simultaneously
6. Work with encrypted shared graphs

**Time**: ~15 minutes  
**Prerequisites**: Logseq Desktop installed, Logseq account (free)

## What is Real-Time Collaboration?

Logseq's Real-Time Collaboration (RTC) lets multiple people edit the same graph simultaneously, with changes appearing instantly for everyone. Unlike traditional file syncing, RTC provides:

- **Instant synchronization** — See remote changes quickly while collaborating
- **Conflict resolution** — Deterministic transaction ordering with stale-write rejection and retry
- **User presence** — See who's online and where they're working
- **End-to-end encryption** — Your data stays private (optional)

**Architecture Overview**:

```txt
┌─────────────────┐   WebSocket    ┌─────────────────┐   WebSocket    ┌─────────────────┐
│   Your Device   │◄──────────────►│  Logseq Server  │◄──────────────►│ Collaborator    │
│                 │    Encrypted   │                 │    Encrypted   │                 │
│   Edit block    ├───────────────►│   Broadcast     ├───────────────►│   See change    │
│   See update    │◄───────────────┤   operation     │◄───────────────┤   Edit block    │
└─────────────────┘                └─────────────────┘                └─────────────────┘
         ↕                                                                      ↕
    Local Database                                                       Local Database
```

Logseq uses a **transaction-based sync approach** with **optimistic concurrency control** on a centralized server (Cloudflare Durable Objects), ensuring consistency across clients. Each client sends batched transactions with a `t-before` value. The server rejects stale transactions, returns checksums with accepted batches, and broadcasts `changed` notifications so clients can pull the latest txs.

## Step 1: Create Your First Remote Graph

### 1.1 Sign Up for Logseq Sync

If you don't have an account yet:

1. Open Logseq Desktop
2. Click your profile icon (top-right) → **Sign In**
3. Choose **Sign up** if you don't have an account
4. Complete registration with email/password or OAuth provider

### 1.2 Create a New Graph

You have two options:

**Option A: Convert Existing Local Graph** (Recommended for testing)

1. Open an existing graph
2. Click **Settings** (⚙️ icon) → **Sync**
3. Click **Enable Logseq Sync**
4. Choose a graph name (e.g., "Team Project")
5. Click **Create Remote Graph**

**Option B: Create New Remote Graph from Scratch**

1. Click **Graph** menu → **Create new graph**
2. Select **Create remote graph**
3. Enter graph name
4. Click **Create**

### 1.3 Enable Encryption (Optional but Recommended)

When creating a remote graph, you'll see an encryption option:

```txt
┌──────────────────────────────────────────────┐
│  Set password for remote graphs              │
│                                              │
│  Password:     [****************]  👁        │
│  Confirm:      [****************]  👁        │
│                                              │
│  ⚠️  Remember this password!                 │
│  ⚠️  We cannot reset or retrieve it.         │
│  ⚠️  Keep a secure backup.                   │
│                                              │
│  [Submit]                                    │
└──────────────────────────────────────────────┘
```

**Important Notes**:

- Choose a strong password (20+ characters)
- **Save it in a password manager** — there's no recovery option
- All collaborators will need their own password
- See [E2EE Encryption Explanation](../../explanation/security/e2ee-encryption.md) for security details

**Result**: After a few moments, your graph will be uploaded and ready for collaboration!

## Step 2: Invite Collaborators

### 2.1 Invite by Email

1. Click the collaborators button (user-plus icon) in the header, or open **Settings** → **Collaboration**
2. In **Members**, enter the collaborator's email address
3. Click **Invite**

> **Removed:** Link-based invitation flow (`https://logseq.com/share/...` with expiry) is not part of the current desktop RTC invitation path. Current flow uses member invites via `POST /graphs/:graph-id/members`.

### 2.2 Send Invitation

Share the invitation email address with your collaborator (the person must have or create a Logseq account for access).

### 2.3 Accept Invitation (Collaborator Side)

When your collaborator is invited:

1. They sign in to Logseq
2. They access the graph from their remote graph list
3. The graph appears in their **Graphs** list
4. If encrypted, they'll need to enter their own password (see Step 2.4)

> **Removed:** Explicit in-app **Accept Invitation** link-confirmation flow is not represented in current RTC member APIs/UI.

### 2.4 Set Up Encryption (If Graph is Encrypted)

When joining an encrypted graph:

```txt
┌──────────────────────────────────────────────┐
│  Enter password for remote graphs            │
│                                              │
│  Password:     [****************]  👁        │
│                                              │
│  [Submit]                                    │
└──────────────────────────────────────────────┘
```

**Important**: Each collaborator sets their **own E2EE password**, which protects their RSA private key. The graph is encrypted with a shared AES key. Each user's copy of the AES key is encrypted with their RSA public key, so everyone can decrypt the graph data using their own password and RSA key pair.

**Result**: Once set up, all collaborators can edit the graph simultaneously!

## Step 3: Your First Collaborative Edit

Let's practice real-time editing together:

### 3.1 Create a Test Page

**You do:**

1. Create a new page: `[[Team Standup]]`
2. Add a heading: `# Today's Updates`
3. Add your name: `- **Your Name**: Working on tutorial`

**What happens**: Your collaborator sees the page appear instantly in their sidebar under "Recent".

### 3.2 Simultaneous Editing

**Ask your collaborator to:**

1. Open the same `[[Team Standup]]` page
2. Add their own update below yours: `- **Their Name**: Testing RTC`

**What you'll see**:

- Their avatar appears in the collaborators area when online
- If they edit a block, that block can show an editing-user avatar indicator
- No conflicts when you're editing different blocks

> **Removed:** Character-by-character shared cursor rendering is not the current RTC presence model; presence is block-level (`editing-block-uuid`).

### 3.3 User Presence

Notice the **online users indicator** at the top-right:

```txt
┌──────────────────────────────────────┐
│  👤 You  🟢 Alice  🟢 Bob            │
└──────────────────────────────────────┘
```

- Online collaborators appear with name/avatar metadata from presence updates.
- Block-level editing presence is shown by an editing avatar on the block.

> **Removed:** Idle-state color semantics and click-to-jump behavior are not guaranteed by the current implementation.

## Step 4: Handling Conflicts

What happens when two people edit the **same block** at the same time?

### 4.1 Create a Conflict

**Exercise**: Try this with your collaborator:

1. Both of you open the same block
2. Both start typing at the same time
3. Both finish editing within 2 seconds

**What happens**:

```txt
Original:    - This is a test block

You type:    - This is a test block for RTC
Alice types: - This is a collaborative test block

Result:      Server accepts one batch at a time by `t-before`.
             If your batch is stale, it is rejected and your client pulls latest txs, then retries.
```

### 4.2 Conflict Resolution Algorithm

Logseq uses **optimistic concurrency control** with transaction versioning:

1. **Local Edit**: You edit a block, generating a local transaction
2. **Send to Server**: Client sends the transaction with `t-before` (the last known server transaction number)
3. **Server Check**: Server verifies `t-before` matches its current `t`
4. **Accept or Reject**: If `t-before` matches, the transaction is applied and broadcast. If stale, the server rejects with the current `t`
5. **Retry**: On rejection, the client pulls the latest changes, merges locally, and retries

**Conflict Strategies**:

| Situation                    | Resolution                                        |
| ---------------------------- | ------------------------------------------------- |
| Different blocks             | No conflict - transactions applied sequentially |
| Same block, concurrent edits | One tx path wins first; stale batch gets `tx/reject` and client retries |
| Stale `t-before`             | Rejected with `reason: "stale"`; client issues `pull` |
| Partial batch apply failure  | `tx/reject` can include `success-tx-ids` and `failed-tx-id` |

## Step 5: Advanced Features

### 5.1 Block-Level Presence

When you start editing a block, the system broadcasts your `editing-block-uuid` to other connected clients via the WebSocket presence protocol. Other users can see which block you are currently editing.

**Not a lock**: Others can still edit the same block. This is purely informational presence tracking, not a locking mechanism.

### 5.2 Offline Editing

RTC gracefully handles offline scenarios:

**When you go offline**:

1. You can continue editing (all changes saved locally)
2. Sync status reflects offline state
3. Changes queue up locally

**When you reconnect**:

1. Local changes sync automatically
2. Remote changes are downloaded
3. Conflicts resolved using same algorithm
4. Sync state returns to connected/open

**Storage**: Local changes and sync metadata are stored in local SQLite-backed client-op tables until synced.

> **Removed:** DataScript-based client-op storage is deprecated and unsupported in the current worker sync client-op implementation.

### 5.3 Asset Synchronization

When you add images or files:

1. Upload to graph: Drag & drop image into block
2. RTC uploads encrypted asset to cloud
3. Other collaborators download automatically
4. Assets cached locally for offline access

**Current behavior**:

- Assets are uploaded/downloaded through the sync asset endpoints
- The sync indicator reports pending uploads/downloads
- Download/upload progress is tracked per asset operation

> **Removed:** This tutorial no longer guarantees a fixed "supported asset types" matrix (or PDF annotation sync semantics) because current sync logic is endpoint/operation based.

### 5.4 Page Moves and Renames

When you rename or move pages:

```txt
Your action:        Rename [[Old Name]] → [[New Name]]
                              ↓
Transaction:        Datascript tx-data updating :block/title and related refs
                              ↓
Server broadcast:   {:type "changed" :t new-t} sent to all clients
                              ↓
Collaborators:      Pull latest transactions, apply page rename + backlinks
```

**Result**: All references updated automatically for everyone.

## Step 6: Best Practices

### 6.1 Communication

**Use Logseq for coordination**:

- Create a `[[Team Coordination]]` page
- Use tasks for assignments: `TODO Review documentation`
- Add comments with block references: `Check this: ((block-ref))`

**External communication**:

- Slack/Discord for quick questions
- Video calls for complex discussions
- Don't rely solely on RTC for communication

### 6.2 Editing Etiquette

✅ **Do**:

- Announce major changes (e.g., in `[[Team Updates]]`)
- Work in different sections when possible
- Use descriptive commit-like notes: `- 2024-01-20: Refactored architecture section`

❌ **Don't**:

- Delete others' work without discussion
- Make massive changes during others' active editing
- Rename pages constantly (affects everyone)

### 6.3 Organization

**Create Structure**:

```markdown
[[Project Hub]]

- [[Meeting Notes]]
  - [[2024-01-20 Standup]]
- [[Documentation]]
  - [[Architecture]]
  - [[API Reference]]
- [[Tasks]]
  - TODO items with assignments
```

**Use Namespaces**:

- `project/backend/api` — Backend API docs
- `project/frontend/ui` — Frontend UI docs
- `project/meeting/weekly` — Weekly meeting notes

### 6.4 Performance Tips

**For large graphs (10,000+ blocks)**:

1. **Split into multiple graphs** — Keep graphs focused (< 20,000 blocks)
2. **Use namespaces** — Helps Logseq optimize loading
3. **Archive old content** — Move to separate "Archive" graph
4. **Avoid excessive references** — Too many backlinks slow down sync

**Monitor sync status**:

Use the cloud sync indicator in the header to inspect:

- Pending local changes
- Pending asset uploads/downloads
- Pending server changes
- Last synced time

```txt
Cloud indicator -> details popup
```

> **Removed:** Settings-based **Sync Status** panel and **Force Full Sync** action are not part of the current desktop RTC indicator flow.

## Troubleshooting

### Sync Issues

**Problem**: Changes not appearing for collaborators

**Solutions**:

1. Check internet connection (RTC requires constant connection)
2. Check the cloud sync indicator details in the header
3. Look for error messages in status bar
4. Try restarting sync from the indicator (**Start sync**) if it is closed
5. Restart Logseq (re-establishes WebSocket connection)

### Conflict Storms

**Problem**: Too many conflicts, constant re-merging

**Cause**: Multiple people editing same blocks simultaneously

**Solutions**:

1. **Coordinate**: Use chat to divide work
2. **Work in sections**: Each person focuses on different pages
3. **Take turns**: For complex edits, announce "I'm editing X page"
4. **Review conflicts**: Check the sync indicator for pending operations and logs

### Password Issues

**Problem**: Forgot E2EE password

**Solution**:

- ⚠️ **No recovery possible** (by design)
- You can still access **local graph** (not encrypted)
- For cloud data: Create new graph with new password
- See [E2EE Encryption Guide](../../explanation/security/e2ee-encryption.md#what-happens-if-you-forget) for details

### Performance Degradation

**Problem**: Laggy typing, slow sync

**Check**:

1. **Graph size**: Settings → About → **Database Size**
   - If > 100 MB, consider splitting graph
2. **Network speed**: RTC requires stable connection (50+ Kbps)
3. **Device resources**: Logseq uses ~200-500 MB RAM for RTC
4. **Browser console**: Check for errors (Help → Toggle Developer Tools)

## What You've Learned

✅ How to create a remote graph with RTC enabled  
✅ How to invite collaborators and set up encryption  
✅ How to edit simultaneously with conflict resolution  
✅ How to understand user presence and editing indicators  
✅ Best practices for team collaboration  
✅ How to troubleshoot common issues

## Next Steps

**Try these:**

1. **Invite a friend** and practice simultaneous editing
2. **Create a team wiki** with organized namespaces
3. **Test offline mode** — disconnect, edit, reconnect
4. **Explore advanced features**:
   - [Sharing public graphs](../../how-to/collaboration/sharing-graphs.md)
   - [User profiles](../../reference/features/user-profiles.md) with avatars
   - [Asset management](../../how-to/multimedia/asset-management.md) for files

**Learn more:**

- [E2EE Encryption Deep Dive](../../explanation/security/e2ee-encryption.md)
- [RTC Architecture (Developer Onboarding)](../../../dz/docs/01-onboarding/rtc-onboarding.md)
- [Conflict Resolution Algorithm](../../explanation/collaboration/conflict-resolution.md)

## Findings: New Capabilities

Since the previous revision, current source reflects these notable capabilities:

1. **Per-entry reject metadata for tx batches**
  - `tx/reject` can return `success-tx-ids` and `failed-tx-id`, enabling precise client reconciliation for partially applied batches.
2. **Checksum-aware sync responses**
  - `hello`, `pull/ok`, and `tx/batch/ok` include checksum fields used for client/server consistency checks (especially in dev/test diagnostics).
3. **Graph readiness gating during bootstrap**
  - Sync and snapshot endpoints can return not-ready states while snapshot upload/import is still in progress (`graph-ready-for-use?`).
4. **Richer sync indicator telemetry**
  - Header indicator surfaces pending local, server, and asset-operation counts plus transfer progress.
5. **Member-management collaboration flow**
  - Collaboration now centers on graph members (`/graphs/:graph-id/members`) with invite/remove operations rather than share-link onboarding.

## Reference: Key Concepts

### Transactions

Every edit generates **Datascript transaction data** that is synchronized:

```clojure
;; Transaction data is a sequence of datoms:
;; [:db/add entity-id :block/title "New content"]
;; [:db/add entity-id :block/updated-at timestamp]
;; [:db/retract entity-id :block/title "Old content"]
```

The client normalizes local Datascript transactions, serializes them as Transit, optionally encrypts them (if E2EE is enabled), and sends them as a `tx/batch` message. The server assigns a monotonically increasing transaction number `t` for ordering.

### Fractional Indexing

Blocks use **fractional indexing** (via `logseq.db.common.order` and `logseq.clj-fractional-indexing`) for the `:block/order` attribute, allowing insertion between any two blocks without renumbering:

```txt
Block A:  order="a0"
Block B:  order="a2"

Insert between A and B:  order="a1"  (generated via generate-key-between)
```

**Advantage**: Two people can insert blocks at different positions without conflict. The `logseq.db-sync.order` module also detects and fixes duplicate order keys that can arise from concurrent edits.

### WebSocket Protocol

Sync uses **WebSocket** for real-time communication. Messages are JSON-encoded with a `type` field:

```txt
Client → Server:   {"type": "hello", "client": "<repo>"}
Client → Server:   {"type": "tx/batch", "t-before": 42, "txs": [{"tx": "<transit>", "tx-id": "<uuid?>"}]}
Client → Server:   {"type": "pull", "since": 42}
Client → Server:   {"type": "presence", "editing-block-uuid": "<uuid>"}
Client → Server:   {"type": "ping"}

Server → Client:   {"type": "hello", "t": 42}
Server → Client:   {"type": "tx/batch/ok", "t": 43}
Server → Client:   {"type": "tx/reject", "reason": "stale", "t": 43}
Server → Client:   {"type": "tx/reject", "reason": "db transact failed", "t": 43, "success-tx-ids": ["..."], "failed-tx-id": "..."}
Server → Client:   {"type": "changed", "t": 43}
Server → Client:   {"type": "pull/ok", "t": 43, "txs": [...]}
Server → Client:   {"type": "online-users", "online-users": [...]}
Server → Client:   {"type": "presence", "user-id": "...", "editing-block-uuid": "..."}
Server → Client:   {"type": "pong"}
```

**Encrypted**: If E2EE is enabled, the `txs` payload within `tx/batch` messages is encrypted before sending using the graph's AES key.

---

**Last Updated**: 2026-09-09  
**Status**: 🟡 Active Development (RTC features evolving rapidly)  
**Feedback**: Join [Logseq Discord](https://discord.gg/logseq) `#rtc-feedback` channel
