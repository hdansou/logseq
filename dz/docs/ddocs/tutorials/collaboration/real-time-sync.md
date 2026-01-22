---
title: 'Getting Started with Real-Time Collaboration'
status: '🟡 Active Development'
version: 'Since v0.9.0'
quadrant: 'Tutorial'
platform: 'Desktop (macOS/Windows/Linux)'
related-docs:
  - '../../explanation/security/e2ee-encryption.md'
  - '../../reference/features/user-profiles.md'
  - '../../how-to/collaboration/sharing-graphs.md'
---

# Getting Started with Real-Time Collaboration

## What You'll Learn

In this tutorial, you'll learn how to:

1. Enable Real-Time Collaboration (RTC) for your graph
2. Invite collaborators to work with you
3. See others' changes in real-time
4. Understand user presence and cursors
5. Resolve conflicts when editing simultaneously
6. Work with encrypted shared graphs

**Time**: ~15 minutes  
**Prerequisites**: Logseq Desktop installed, Logseq account (free)

## What is Real-Time Collaboration?

Logseq's Real-Time Collaboration (RTC) lets multiple people edit the same graph simultaneously, with changes appearing instantly for everyone. Unlike traditional file syncing, RTC provides:

- **Instant synchronization** — See changes as they happen (< 1 second)
- **Conflict resolution** — Smart merging when multiple people edit the same content
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

Unlike CRDT-based systems (Conflict-free Replicated Data Types), Logseq uses an **operation-based approach** with **centralized conflict resolution**, ensuring consistency across all clients.

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

### 2.1 Generate Share Link

1. Click **Settings** (⚙️) → **Sync** tab
2. Scroll to **Collaboration** section
3. Click **Invite collaborators**
4. A share link is generated (valid for 7 days)

**Share Link Format**:

```
https://logseq.com/share/abc123def456?expires=1234567890
```

### 2.2 Send Invitation

Copy the link and send it to your collaborators via:

- Email
- Slack/Discord
- Any secure messaging app

**Security Tip**: Share links expire after 7 days. For ongoing collaboration, invite via Logseq account email instead.

### 2.3 Accept Invitation (Collaborator Side)

When your collaborator receives the link:

1. They click the link (opens Logseq)
2. If not signed in, they'll be prompted to create a Logseq account
3. They click **Accept Invitation**
4. The graph appears in their **Graphs** list
5. If encrypted, they'll need to enter their own password (see Step 2.4)

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

**Important**: Each collaborator sets their **own password** for the shared graph. This is different from the graph creator's password. All users share the same encryption key but protect it with individual passwords.

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

- Their cursor appears as a colored indicator (e.g., 🟦 "Alice")
- Their text appears as they type (character by character)
- No conflicts — you're editing different blocks

### 3.3 User Presence

Notice the **online users indicator** at the top-right:

```txt
┌──────────────────────────────────────┐
│  👤 You  🟢 Alice  🟢 Bob            │
└──────────────────────────────────────┘
```

- **Green dot 🟢**: Online and active
- **Gray dot ⚪**: Online but idle (5+ minutes inactive)
- **No indicator**: Offline

Click on a name to see where they're currently editing.

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

Result:      - This is a collaborative test block for RTC
             (Merged intelligently by conflict resolution)
```

### 4.2 Conflict Resolution Algorithm

Logseq uses **operation-based conflict resolution** with timestamps:

1. **Local Operation**: You edit → Generate operation with timestamp `t1`
2. **Remote Operation**: Alice edits → Generate operation with timestamp `t2`
3. **Server Ordering**: Server receives both, orders by timestamp
4. **Merge**: Earlier operation applied first, later operation reconciled
5. **Broadcast**: Result sent to all clients

**Conflict Strategies**:

| Situation                    | Resolution                         |
| ---------------------------- | ---------------------------------- |
| Different blocks             | No conflict — both changes applied |
| Same block, different words  | Text merged intelligently          |
| Same block, overlapping text | Last edit wins (higher timestamp)  |
| Block deleted + edited       | Deletion wins (safer)              |

### 4.3 Manual Conflict Review

Sometimes you'll see a conflict marker:

```markdown
- Original text
  <<<<<<< Your changes
  Your edit here
  =======
  Their edit here
  > > > > > > > Alice's changes
```

**To resolve**:

1. Review both versions
2. Edit to combine the best of both
3. Delete the conflict markers
4. Save — everyone sees the merged result

## Step 5: Advanced Features

### 5.1 Block-Level Locking (Soft Lock)

When you start editing a block, others see a **visual indicator**:

```txt
┌─────────────────────────────────────────────────┐
│  - This is a block [🔒 You are editing]        │
└─────────────────────────────────────────────────┘
```

**Not a hard lock**: Others can still edit, but they'll see your changes in real-time and know you're actively working there.

### 5.2 Offline Editing

RTC gracefully handles offline scenarios:

**When you go offline**:

1. You can continue editing (all changes saved locally)
2. A **yellow indicator** shows you're offline
3. Changes queue up locally

**When you reconnect**:

1. Local changes sync automatically
2. Remote changes are downloaded
3. Conflicts resolved using same algorithm
4. Green indicator shows you're back online

**Storage**: Local changes stored in IndexedDB until synced.

### 5.3 Asset Synchronization

When you add images or files:

1. Upload to graph: Drag & drop image into block
2. RTC uploads encrypted asset to cloud
3. Other collaborators download automatically
4. Assets cached locally for offline access

**Supported Asset Types**:

- Images: PNG, JPG, GIF, SVG, WebP
- Documents: PDF (with annotations synced)
- Videos: MP4, WebM (embedded via YouTube/Vimeo)
- Files: Any file type (stored as attachment)

### 5.4 Page Moves and Renames

When you rename or move pages:

```txt
Your action:        Rename [[Old Name]] → [[New Name]]
                              ↓
RTC operation:      [:update-page t {:page-uuid uuid :new-name "New Name"}]
                              ↓
Server broadcast:   All clients update page name + backlinks
                              ↓
Collaborators see:  Page renamed everywhere, backlinks updated
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

```txt
Settings → Sync → Sync Status
  Last synced: 2 seconds ago ✅
  Pending operations: 0
  Network: Connected 🟢
```

## Troubleshooting

### Sync Issues

**Problem**: Changes not appearing for collaborators

**Solutions**:

1. Check internet connection (RTC requires constant connection)
2. Check sync status: Settings → Sync → **Sync Status**
3. Look for error messages in status bar
4. Try **Force Sync**: Settings → Sync → **Force Full Sync**
5. Restart Logseq (re-establishes WebSocket connection)

### Conflict Storms

**Problem**: Too many conflicts, constant re-merging

**Cause**: Multiple people editing same blocks simultaneously

**Solutions**:

1. **Coordinate**: Use chat to divide work
2. **Work in sections**: Each person focuses on different pages
3. **Take turns**: For complex edits, announce "I'm editing X page"
4. **Review conflicts**: Use git-style diff to understand what happened

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
✅ How to understand user presence and cursors  
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

## Reference: Key Concepts

### Operations

Every edit generates an **operation** that's synchronized:

```clojure
;; Update block content
[:update t {:block-uuid uuid :av-coll [[attr value] ...]}]

;; Move block to new position
[:move t {:block-uuid uuid :parent-uuid uuid :left-uuid uuid}]

;; Delete block
[:remove t {:block-uuid uuid}]

;; Update page properties
[:update-page t {:block-uuid uuid :av-coll [...]}]
```

**Transaction Timestamp** (`t`): Determines order for conflict resolution.

### Fractional Indexing

Blocks use **fractional indexes** for ordering without conflicts:

```txt
Block A:  index="a0"
Block B:  index="a2"

Insert between A and B:  index="a1"  (generated automatically)
```

**Advantage**: Two people can insert blocks at the same position without conflict — server generates unique indexes.

### WebSocket Protocol

RTC uses **WebSocket** for real-time communication:

```txt
Client → Server:   {:action "apply-ops" :ops [...]}
Server → Client:   {:req-id "push-updates" :value {:ops [...]}}
```

**Encrypted**: If E2EE enabled, operations are encrypted before sending.

---

**Last Updated**: January 2026  
**Status**: 🟡 Active Development (RTC features evolving rapidly)  
**Feedback**: Join [Logseq Discord](https://discord.gg/logseq) `#rtc-feedback` channel
