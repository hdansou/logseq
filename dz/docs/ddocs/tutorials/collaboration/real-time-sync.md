---
title: 'Getting Started with Real-Time Collaboration'
status: '🟡 Active Development'
version: 'Beta (app 2.0.1)'
quadrant: 'Tutorial'
platform: 'Desktop (macOS/Windows/Linux)'
graphType: [db]
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
related-docs:
  - '../../explanation/security/e2ee-encryption.md'
  - '../../reference/features/user-profiles.md'
  - '../../reference/features/sync-limits.md'
  - '../../explanation/comparisons/rtc-vs-file-sync.md'
---

# Getting Started with Real-Time Collaboration

> **Last revised:** 2026-09-11

## What You'll Learn

In this tutorial, you'll learn how to:

1. Turn on Logseq Sync for a DB graph (new or existing)
2. Invite collaborators and remove them
3. See others' changes arrive in your graph
4. Read the online-user and block-editing indicators
5. Recognize and resolve a sync conflict
6. Work with end-to-end encrypted (E2EE) graphs

**Time**: ~15 minutes
**Prerequisites**: Logseq Desktop, a DB graph, a Logseq account **with sync access** (see [Who Can Use Sync](#who-can-use-sync)), and a collaborator whose account also has sync access

## Who Can Use Sync

Sync is in beta. The app's own menu label is **Use Logseq Sync (Beta testing)** ([en.edn:785](../../../../../src/resources/dicts/en.edn#L785)).

Every sync entry point is gated by `rtc-group?` ([user.cljs:357-362](../../../../../src/main/frontend/handler/user.cljs#L357-L362)), which is true when **any** of these holds:

- the app is a development build
- a custom **Sync Server URL** is set in **Settings → Advanced** ([config.cljs:51-56](../../../../../src/main/frontend/config.cljs#L51-L56), [settings.cljs:927](../../../../../src/main/frontend/components/settings.cljs#L927))
- your account is in the user group `team` or `rtc_2025_07_10`

If `rtc-group?` is false you do not get the sync checkboxes when creating a graph ([repo.cljs:700-723](../../../../../src/main/frontend/components/repo.cljs#L700-L723)), the upload menu item or header cloud button ([repo.cljs:40-47](../../../../../src/main/frontend/components/repo.cljs#L40-L47), [repo.cljs:248-257](../../../../../src/main/frontend/components/repo.cljs#L248-L257)), the remote-graph lists ([repo.cljs:350-374](../../../../../src/main/frontend/components/repo.cljs#L350-L374)), or the header sync indicator ([header.cljs:98-108](../../../../../src/main/frontend/components/header.cljs#L98-L108)). The same gate applies to your collaborators: an invited account without sync access does not see the shared graph in its graph list.

> **Unverified:** how accounts are added to those groups, and which plan, price or "Pro" tier (if any) grants access. Group membership comes from the account server, outside this repository.

Note that **Settings → Collaboration** is shown to every logged-in user, whether or not they have sync access ([settings.cljs:1487-1488](../../../../../src/main/frontend/components/settings.cljs#L1487-L1488)).

## What is Real-Time Collaboration?

In source this feature is called **db-sync**: the client lives in `src/main/frontend/worker/sync/`, the server in `deps/db-sync/`. Every collaborator keeps a full local copy of the graph. Edits go to a Logseq server over a WebSocket (`wss://api.logseq.io/sync/...`, [config.cljs:41-44](../../../../../src/main/frontend/config.cljs#L41-L44)), which orders them and tells the other clients to pull.

- **Near-real-time updates** — once the server accepts a batch it broadcasts `changed`, and the other clients pull the new transactions ([sync.cljs:629-639](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L629-L639))
- **Server-ordered transactions** — a batch based on an out-of-date server state is rejected, and the client pulls before trying again. There is no CRDT.
- **Presence** — who is online, and which block each person is editing
- **Optional end-to-end encryption** — chosen per graph

**Architecture Overview**:

```txt
┌─────────────────┐  WebSocket    ┌─────────────────┐  WebSocket    ┌─────────────────┐
│   Your Device   │◄─────────────►│  Logseq Server  │◄─────────────►│  Collaborator   │
│                 │  (wss / TLS)  │                 │  (wss / TLS)  │                 │
│   Edit block    ├──────────────►│  Order + store  ├──────────────►│  Pull + apply   │
│   Pull + apply  │◄──────────────┤  "changed"      │◄──────────────┤  Edit block     │
└─────────────────┘               └─────────────────┘               └─────────────────┘
         ↕                                                                    ↕
    Local graph                                                          Local graph
```

The hosted server is a Cloudflare Worker built on Durable Objects ([worker.cljs:4](../../../../../deps/db-sync/src/logseq/db_sync/worker.cljs#L4), [:25](../../../../../deps/db-sync/src/logseq/db_sync/worker.cljs#L25)). `deps/db-sync` also ships a Node server entry point (`deps/db-sync/src/logseq/db_sync/node/server.cljs`). Each client sends batched transactions tagged with `t-before`, the last server transaction number it has seen. The server rejects the batch if `t-before` is not its current `t` ([sync.cljs:624-627](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L624-L627)). Otherwise it applies the batch, replies `tx/batch/ok` with the new `t` (plus a checksum), and broadcasts `changed` to the other clients ([sync.cljs:629-639](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L629-L639)).

## Step 1: Create Your First Remote Graph

### 1.1 Sign In

If you are not signed in yet:

1. Open Logseq Desktop
2. Open the **⋯** menu at the top right of the header and choose **Login**. The item only appears while you are logged out ([header.cljs:242-244](../../../../../src/main/frontend/components/header.cljs#L242-L244), [:273](../../../../../src/main/frontend/components/header.cljs#L273); [en.edn:1884](../../../../../src/resources/dicts/en.edn#L1884))
3. Enter your email and password and click **Sign In**. If you have no account yet, choose **Sign up** and then **Create Account** ([login.cljs:136-145](../../../../../src/main/frontend/components/user/login.cljs#L136-L145), [:186](../../../../../src/main/frontend/components/user/login.cljs#L186))

### 1.2 Put a Graph on the Server

You have two options.

**Option A: Upload an Existing Local Graph**

1. Open the local DB graph. It must be the current graph.
2. Click the **cloud** button in the header (tooltip **Use Logseq Sync (Beta testing)**, [header.cljs:111-118](../../../../../src/main/frontend/components/header.cljs#L111-L118), [:589-590](../../../../../src/main/frontend/components/header.cljs#L589-L590)). Or open **All graphs**, open that graph's menu, and choose **Use Logseq Sync (Beta testing)** ([repo.cljs:248-257](../../../../../src/main/frontend/components/repo.cljs#L248-L257)).
3. In the **Upload graph "<name>" to Logseq Server?** dialog, click **Confirm** ([repo.cljs:78-84](../../../../../src/main/frontend/components/repo.cljs#L78-L84); [en.edn:784](../../../../../src/resources/dicts/en.edn#L784), [:1863](../../../../../src/resources/dicts/en.edn#L1863))

This flow has no encryption checkbox. For the current graph, the upload counts as encrypted unless the graph's stored E2EE flag is `false`. A graph that has never been synced has no flag, and that also counts as encrypted ([repo.cljs:59-70](../../../../../src/main/frontend/components/repo.cljs#L59-L70)).

**Option B: Create a New Graph with Sync On**

1. On **All graphs**, click **Create a new graph** ([repo.cljs:336-342](../../../../../src/main/frontend/components/repo.cljs#L336-L342)). Or use the graph switcher's **Create db graph** button, which is labelled **Create a new graph** outside Electron ([repo.cljs:431](../../../../../src/main/frontend/components/repo.cljs#L431)).
2. Type a graph name
3. Tick **Use Logseq Sync?**. A second checkbox, **Encrypt graph data**, appears; it is ticked by default ([repo.cljs:646](../../../../../src/main/frontend/components/repo.cljs#L646), [:700-723](../../../../../src/main/frontend/components/repo.cljs#L700-L723); [en.edn:745](../../../../../src/resources/dicts/en.edn#L745), [:786](../../../../../src/resources/dicts/en.edn#L786))
4. Click **Submit**. With encryption on, the button stays disabled until your E2EE key pair is ready ([repo.cljs:725](../../../../../src/main/frontend/components/repo.cljs#L725))

Logseq creates the local graph, then creates the remote graph and starts sync ([sync.cljs:456-463](../../../../../src/main/frontend/handler/db_based/sync.cljs#L456-L463)).

### 1.3 Set Your Encryption Password

The first time you work with an encrypted graph and have no E2EE key pair yet, Logseq asks you to create a password ([events/rtc.cljs:46-59](../../../../../src/main/frontend/handler/events/rtc.cljs#L46-L59), [e2ee.cljs:13-58](../../../../../src/main/frontend/components/e2ee.cljs#L13-L58)):

```txt
┌──────────────────────────────────────────────────┐
│  Set password for remote graphs                  │
│                                                  │
│  [ Enter password         ]  👁                  │
│  [ Enter password again   ]  👁                  │
│                                                  │
│  🔑 Please make sure you remember the password   │
│     you have set, as we are unable to reset or   │
│     retrieve it if you forget it, and we         │
│     recommend you keep a secure backup of the    │
│     password.                                    │
│  🔒 If you lose your password, all of your data  │
│     in the cloud can't be decrypted. You will    │
│     still be able to access the local version    │
│     of your graph.                               │
│                                                  │
│  [Submit]                                        │
└──────────────────────────────────────────────────┘
```

(Text from [en.edn:600](../../../../../src/resources/dicts/en.edn#L600), [:602-603](../../../../../src/resources/dicts/en.edn#L602-L603), [:618](../../../../../src/resources/dicts/en.edn#L618), [:621](../../../../../src/resources/dicts/en.edn#L621).)

**Important Notes**:

- The password protects **your** RSA private key, not one particular graph. One password covers every encrypted graph you use.
- Save it in a password manager. There is no recovery, only a destructive force reset (see [Password Issues](#password-issues)).
- Encryption covers block and page text (`:block/title`, `:block/name`, [const.cljs:25-27](../../../../../src/main/frontend/worker/sync/const.cljs#L25-L27)) and asset files ([assets.cljs:26-33](../../../../../src/main/frontend/worker/sync/assets.cljs#L26-L33)). Other attributes are not in that set. See [E2EE Encryption Explanation](../../explanation/security/e2ee-encryption.md) for the details.
- After you enter the password once, Logseq saves it locally and tries the saved copy first in later sessions ([e2ee.cljs:89-100](../../../../../src/main/frontend/components/e2ee.cljs#L89-L100), [events/rtc.cljs:26-44](../../../../../src/main/frontend/handler/events/rtc.cljs#L26-L44))

**Result**: Upload progress shows in the header. When it finishes, the cloud sync indicator appears. Until a new graph's snapshot has uploaded, the graph list marks it **Preparing** ([repo.cljs:385-395](../../../../../src/main/frontend/components/repo.cljs#L385-L395); [en.edn:766](../../../../../src/resources/dicts/en.edn#L766)), and the server rejects edits with `snapshot upload in progress` ([sync.cljs:615-618](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L615-L618)).

## Step 2: Invite Collaborators

### 2.1 Invite by Email

Only the graph's **manager** can invite. The manager is the account that created the remote graph ([handler/index.cljs:134](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/index.cljs#L134)).

1. Click the **user-plus** button in the header, which opens a **Members:** dialog ([header.cljs:127-132](../../../../../src/main/frontend/components/header.cljs#L127-L132)). Or open **Settings → Collaboration**.
2. Type your collaborator's address into **Email address**
3. Click **Invite**, or press Enter ([settings.cljs:1197-1273](../../../../../src/main/frontend/components/settings.cljs#L1197-L1273); [en.edn:274-277](../../../../../src/resources/dicts/en.edn#L274-L277))

On success you see **Invitation sent!** ([en.edn:1837](../../../../../src/resources/dicts/en.edn#L1837)). What actually happens ([sync.cljs:378-414](../../../../../src/main/frontend/handler/db_based/sync.cljs#L378-L414)):

- The server looks up an **existing** Logseq account by that email and adds it to the graph with the role `member` ([sync.cljs:385-387](../../../../../src/main/frontend/handler/db_based/sync.cljs#L385-L387), [handler/index.cljs:162-195](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/index.cljs#L162-L195)). That handler only writes the membership. It sends no email, creates no share link, and sets no expiry.
- If no account uses the address, you see **User doesn't exist yet.** ([handler/index.cljs:193](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/index.cljs#L193), [sync.cljs:401-403](../../../../../src/main/frontend/handler/db_based/sync.cljs#L401-L403)). Ask your collaborator to sign up first, then invite them again.
- The invite field is shown to every member, but the server refuses invites from non-managers with 403 ([handler/index.cljs:186-191](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/index.cljs#L186-L191)). The app then shows the generic **Something wrong, please try again.** ([sync.cljs:404-409](../../../../../src/main/frontend/handler/db_based/sync.cljs#L404-L409); [en.edn:1849](../../../../../src/resources/dicts/en.edn#L1849)).
- For an **encrypted** graph, your client also wraps the graph's AES key with the invitee's RSA public key ([sync.cljs:113-118](../../../../../src/main/frontend/handler/db_based/sync.cljs#L113-L118), [crypt.cljs:666-695](../../../../../src/main/frontend/worker/sync/crypt.cljs#L666-L695)). If the invitee has never set an E2EE password, they have no public key and that step fails ([crypt.cljs:678-679](../../../../../src/main/frontend/worker/sync/crypt.cljs#L678-L679)), so you see **Something wrong, please try again.** The membership was already created by the earlier request, though ([sync.cljs:392-398](../../../../../src/main/frontend/handler/db_based/sync.cljs#L392-L398)). Have the invitee set their E2EE password first, then invite again.

### 2.2 Roles

There are two roles, `manager` and `member` ([handler/index.cljs:134](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/index.cljs#L134), [:176](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/index.cljs#L176)).

| Action | `manager` | `member` |
| ------ | --------- | -------- |
| Invite by email | Yes | No (server returns 403) |
| Remove someone | Members only, not other managers ([handler/index.cljs:224-244](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/index.cljs#L224-L244)) | Only themselves (leave) |
| **Delete from server** (graph menu on All graphs) | Shown ([repo.cljs:259-278](../../../../../src/main/frontend/components/repo.cljs#L259-L278)) | Not shown |
| **Leave this graph** (graph menu on All graphs) | Not shown | Shown ([repo.cljs:280-311](../../../../../src/main/frontend/components/repo.cljs#L280-L311)) |

The server has an endpoint for changing a member's role ([handler/index.cljs:197-222](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/index.cljs#L197-L222)), but nothing in the app calls it, so there is no role-change UI.

### 2.3 Remove a Collaborator

As manager, open the Members panel, click the **⋯** button next to a member, and choose **Remove access** ([settings.cljs:1233-1265](../../../../../src/main/frontend/components/settings.cljs#L1233-L1265); [en.edn:277](../../../../../src/resources/dicts/en.edn#L277)). The button only appears next to members, and only when you are a manager.

> **Unverified:** what happens to the copy of the graph already on the removed person's device. No code that deletes it was found. Removing someone does not rotate an encrypted graph's AES key; see [E2EE Encryption Explanation](../../explanation/security/e2ee-encryption.md#password-rotation-and-reset).

### 2.4 Join the Graph (Collaborator Side)

There is no accept step: the invite adds the membership directly.

1. Your collaborator signs in (Step 1.1)
2. They open **All graphs**. Remote graph lists only load when you are logged in and have sync access ([repo.cljs:327-331](../../../../../src/main/frontend/components/repo.cljs#L327-L331)). The graph appears under **Shared by others:** ([repo.cljs:324-326](../../../../../src/main/frontend/components/repo.cljs#L324-L326), [:368-373](../../../../../src/main/frontend/components/repo.cljs#L368-L373); [en.edn:779](../../../../../src/resources/dicts/en.edn#L779))
3. Clicking the graph downloads it ([repo.cljs:197-205](../../../../../src/main/frontend/components/repo.cljs#L197-L205), [events.cljs:466-475](../../../../../src/main/frontend/handler/events.cljs#L466-L475))
4. If it is encrypted, they enter their own E2EE password (Step 2.5)

### 2.5 Passwords for Encrypted Shared Graphs

A collaborator who already has an E2EE key pair, but whose password isn't saved on this device, sees:

```txt
┌──────────────────────────────────────────────┐
│  Enter password for remote graphs            │
│                                              │
│  [ ************************ ]  👁            │
│                                              │
│  [Submit]                                    │
└──────────────────────────────────────────────┘
```

A wrong entry shows **Wrong password** ([e2ee.cljs:89-130](../../../../../src/main/frontend/components/e2ee.cljs#L89-L130); [en.edn:604](../../../../../src/resources/dicts/en.edn#L604), [:623](../../../../../src/resources/dicts/en.edn#L623)). Someone who has never set a password gets the **Set password for remote graphs** dialog from Step 1.3 instead.

**How this works**: Each user has an RSA-OAEP key pair. The private key is encrypted with an AES-GCM key derived from the user's password via PBKDF2 ([common/crypt.cljs:60-110](../../../../../src/main/frontend/common/crypt.cljs#L60-L110)). The graph itself is encrypted with one AES key, and each member stores a copy of that key wrapped with their own RSA public key ([crypt.cljs:666-695](../../../../../src/main/frontend/worker/sync/crypt.cljs#L666-L695)). So each person uses their **own** password, and no password is ever shared.

**Result**: Once set up, all collaborators can edit the graph at the same time.

## Step 3: Your First Collaborative Edit

Let's practice real-time editing together.

### 3.1 Create a Test Page

**You do:**

1. Create a page named `Team Standup`
2. Add a block: `Your Name: working on the sync tutorial`

**What happens**: Your client sends the change, the server broadcasts `changed`, and your collaborator's client pulls it ([handle_message.cljs:126-127](../../../../../src/main/frontend/worker/sync/handle_message.cljs#L126-L127)). When they search for and open `Team Standup`, your block is already there.

### 3.2 Simultaneous Editing

**Ask your collaborator to:**

1. Open the same `Team Standup` page
2. Add their own block below yours: `Their Name: testing sync`

**What you'll see**:

- Everyone online appears as an initials avatar in the header, next to the user-plus button ([header.cljs:135-149](../../../../../src/main/frontend/components/header.cljs#L135-L149)). Hovering an avatar shows that person's email, masked by default ([header.cljs:146](../../../../../src/main/frontend/components/header.cljs#L146), [util/email.cljs:28-35](../../../../../src/main/frontend/util/email.cljs#L28-L35)).
- While someone edits a block, a small avatar with their initials appears in that block's bullet area ([block.cljs:2234-2244](../../../../../src/main/frontend/components/block.cljs#L2234-L2244), [:2284-2285](../../../../../src/main/frontend/components/block.cljs#L2284-L2285)). You never see your own, and each block shows only one editor ([events.cljs:425-434](../../../../../src/main/frontend/handler/events.cljs#L425-L434)).
- Editing different blocks never produces a conflict entry. Only concurrent changes to the same block's text are tracked ([apply_txs.cljs:1130-1131](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1130-L1131)).

Presence is block-level. There are no shared cursors or selections.

### 3.3 User Presence

The header shows who is connected:

```txt
┌──────────────────────────────────────┐
│  [👤+]  (YO)  (AL)  (BO)             │
└──────────────────────────────────────┘
```

- The list comes from the server's `online-users` message and includes you ([presence.cljs:34-40](../../../../../deps/db-sync/src/logseq/db_sync/worker/presence.cljs#L34-L40)).
- The name is the username from the account token, falling back to the name and then the user id ([presence.cljs:61-71](../../../../../src/main/frontend/worker/sync/presence.cljs#L61-L71)).
- Avatars are always initials: the header passes no image. The background colour comes from each user's UUID.
- There are no idle or away states, and clicking an avatar does not jump to that person.

## Step 4: Handling Conflicts

What happens when two people change the **same block**?

### 4.1 Create a Conflict

**Exercise**: Try this with your collaborator:

1. Both open the same block
2. One of you disconnects from the network. Both of you change the block's text.
3. Reconnect

**What happens**:

```txt
Original:    This is a test block
Alice:       This is a collaborative test block   (reached the server first)
You:         This is a test block for sync        (still pending on your device)

1. Your client pulls Alice's transaction while your change is still pending
2. It records Alice's text as a sync conflict for that block
3. It undoes your pending change, applies Alice's, then re-applies yours on top
4. The block shows YOUR text, plus a warning button for the conflict
```

This sequence is in `apply-remote-tx-with-local-changes!` ([apply_txs.cljs:1673-1697](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1673-L1697)). The conflict check compares your pending title changes with incoming ones ([apply_txs.cljs:1190-1210](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1190-L1210)).

### 4.2 Resolve It

1. Click the ⚠ button on the block (tooltip **Show sync conflicts**, [block.cljs:3389-3416](../../../../../src/main/frontend/components/block.cljs#L3389-L3416))
2. The **Sync conflicts** popup says: "These server versions conflicted with local edits. Copy the text you want and edit the block manually." ([block.cljs:3375-3387](../../../../../src/main/frontend/components/block.cljs#L3375-L3387); [en.edn:1831-1832](../../../../../src/resources/dicts/en.edn#L1831-L1832))
3. Use **Copy** on the server version you want ([block.cljs:3359-3373](../../../../../src/main/frontend/components/block.cljs#L3359-L3373)) and edit the block by hand
4. Click **Mark resolved** ([en.edn:1839](../../../../../src/resources/dicts/en.edn#L1839)). This clears the stored conflicts for that block.

Conflicts are kept in a local `sync_conflicts` SQLite table ([client_op.cljs:63-74](../../../../../src/main/frontend/worker/sync/client_op.cljs#L63-L74)), so each device keeps its own.

> ⚠️ **Watch out:** if a pending local change cannot be re-applied on top of the remote state (the rebase fails or turns into a no-op), Logseq marks it as no longer pending. It is not retried ([apply_txs.cljs:1700-1710](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L1700-L1710)). Only text (`:block/title`) changes show up in the conflicts popup.

### 4.3 How Ordering Works

Logseq uses **optimistic concurrency control** with a server transaction number `t`:

1. **Local edit**: The edit is applied locally and queued as a pending transaction in the client-op store ([client_op.cljs:1-2](../../../../../src/main/frontend/worker/sync/client_op.cljs#L1-L2))
2. **Send to server**: The client sends a `tx/batch` with `t-before` set to the last server `t` it knows ([apply_txs.cljs:994-997](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L994-L997))
3. **Server check**: The server compares `t-before` with its current `t` ([sync.cljs:624-627](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L624-L627))
4. **Accept or reject**: On a match, the server applies the batch, replies `tx/batch/ok`, and broadcasts `changed`. Otherwise it replies `tx/reject` with reason `stale` and its current `t`.
5. **Catch up**: On `stale`, the client requests a pull ([handle_message.cljs:190-192](../../../../../src/main/frontend/worker/sync/handle_message.cljs#L190-L192)). It then applies the remote transactions and rebases its pending ones, as in 4.1.

**Outcomes**:

| Situation | What happens |
| --------- | ------------ |
| Different blocks | Applied in server order; no conflict entry |
| Same block's text, concurrent | Your change is re-applied on top; the server version is kept in **Sync conflicts** |
| Stale `t-before` | `tx/reject` with `reason: "stale"`; client pulls |
| Server fails part of a batch | `tx/reject` with `reason: "db transact failed"`, which may include `success-tx-ids`, `failed-tx-id` and `missing-block-uuids` ([sync.cljs:640-655](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L640-L655)). The client marks the successful ones done and rolls back the failed one ([handle_message.cljs:217-223](../../../../../src/main/frontend/worker/sync/handle_message.cljs#L217-L223)). |
| Graph snapshot still uploading | `tx/reject` with `reason: "snapshot upload in progress"` ([sync.cljs:615-618](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L615-L618)) |

## Step 5: Advanced Features

### 5.1 Block-Level Presence

When you start editing a block, the app sends your `editing-block-uuid` to the server. The call is debounced ([sync.cljs:241-249](../../../../../src/main/frontend/handler/db_based/sync.cljs#L241-L249), [worker/sync.cljs:179-185](../../../../../src/main/frontend/worker/sync.cljs#L179-L185)). The server rebroadcasts it to the other clients with your user id ([ws.cljs:24-30](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/ws.cljs#L24-L30)).

**Not a lock**: The presence handler only records and rebroadcasts. Others can still edit the same block.

### 5.2 Offline Editing

**When you go offline**:

1. You can keep editing. Changes go into your local graph.
2. The indicator details show **Offline**, and the **pending local changes** count grows ([indicator.cljs:207-208](../../../../../src/main/frontend/components/rtc/indicator.cljs#L207-L208); [en.edn:1843](../../../../../src/resources/dicts/en.edn#L1843), [:1846](../../../../../src/resources/dicts/en.edn#L1846))

**When you reconnect**:

1. The WebSocket reconnects with backoff: 1 s, doubling up to 30 s, with jitter ([worker/sync.cljs:22-24](../../../../../src/main/frontend/worker/sync.cljs#L22-L24), [transport.cljs:84-89](../../../../../src/main/frontend/worker/sync/transport.cljs#L84-L89))
2. Client and server exchange `hello` messages carrying the server `t` ([worker/sync.cljs:329](../../../../../src/main/frontend/worker/sync.cljs#L329), [ws.cljs:15-19](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/ws.cljs#L15-L19))
3. Remote changes are pulled, and your pending changes are rebased and sent (Step 4)

**Storage**: Pending changes and sync metadata live in SQLite client-op tables until the server acknowledges them ([client_op.cljs:1-2](../../../../../src/main/frontend/worker/sync/client_op.cljs#L1-L2)).

### 5.3 Asset Synchronization

When you add images or files:

1. Add the file to a block, for example by drag and drop
2. When the asset's checksum is saved, the client queues an `:update-asset` operation ([asset_db_listener.cljs:11-25](../../../../../src/main/frontend/worker/sync/asset_db_listener.cljs#L11-L25)) and uploads the file. In an E2EE graph the file is encrypted with the graph's AES key first ([assets.cljs:26-33](../../../../../src/main/frontend/worker/sync/assets.cljs#L26-L33)).
3. Other collaborators' clients download it

**Limits and gaps**:

- The desktop app refuses to add a file over 100 MB ([handler/assets.cljs:14-17](../../../../../src/main/frontend/handler/assets.cljs#L14-L17))
- The sync client drops any asset upload over 100 MiB, in every graph ([assets.cljs:19](../../../../../src/main/frontend/worker/sync/assets.cljs#L19), [:246-255](../../../../../src/main/frontend/worker/sync/assets.cljs#L246-L255)), even though the server would accept up to 200 MiB for E2EE graphs ([handler/assets.cljs:9-10](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/assets.cljs#L9-L10))
- Deleting an asset does not delete it from the server: only `:update-asset` operations are ever generated ([asset_db_listener.cljs:11-25](../../../../../src/main/frontend/worker/sync/asset_db_listener.cljs#L11-L25))
- The indicator lists pending asset uploads, uploading and downloading counts, and missing asset files ([indicator.cljs:84-96](../../../../../src/main/frontend/components/rtc/indicator.cljs#L84-L96); [en.edn:1827-1830](../../../../../src/resources/dicts/en.edn#L1827-L1830), [:1840-1841](../../../../../src/resources/dicts/en.edn#L1840-L1841), [:1845](../../../../../src/resources/dicts/en.edn#L1845))

See [Sync Limits](../../reference/features/sync-limits.md) for every limit.

### 5.4 Page Renames

In DB graphs, references store UUIDs, not titles. Block refs are `[[<uuid>]]` ([ref.cljs:5-7](../../../../../src/main/frontend/util/ref.cljs#L5-L7)), and typed `[[Title]]` page refs are converted to id refs ([content.cljs:129](../../../../../deps/db/src/logseq/db/frontend/content.cljs#L129)). A rename therefore only changes the page entity:

```txt
Your action:        Rename "Old Name" → "New Name"
                              ↓
Transaction:        :block/title (and :block/name) updated on the page entity
                              ↓
Server broadcast:   {"type": "changed", "t": <new t>} to the other clients
                              ↓
Collaborators:      Pull the transaction; every reference renders the new title
```

**Result**: No other block has to be rewritten, and every reference shows the new name for everyone.

## Step 6: Best Practices

### 6.1 Communication

**Use Logseq for coordination**:

- Create a `Team Coordination` page
- Use tasks for assignments
- To point at a specific block, use **Copy block ref** from the block's context menu ([content.cljs:292](../../../../../src/main/frontend/components/content.cljs#L292); [en.edn:116](../../../../../src/resources/dicts/en.edn#L116)) and paste it. It pastes as `[[<uuid>]]`; the old `((uuid))` form is shown as literal text in DB graphs.

**External communication**:

- Slack/Discord for quick questions
- Video calls for complex discussions
- Don't rely on the graph alone for communication

### 6.2 Editing Etiquette

✅ **Do**:

- Announce major changes (e.g., on a `Team Updates` page)
- Work in different sections when possible
- Leave short dated notes for big edits: `2026-09-11: Refactored architecture section`

❌ **Don't**:

- Delete others' work without discussion. Sync is not a backup, and deletions replicate to everyone. Desktop keeps its own hourly local backups in `<graph>/backup/`, 12 at most ([electron/db.cljs:15-18](../../../../../src/electron/electron/db.cljs#L15-L18)).
- Make massive changes while others are actively editing
- Rename pages constantly (it affects everyone)

### 6.3 Organization

**Create Structure**:

```markdown
Project Hub

- Meeting Notes
  - 2026-09-11 Standup
- Documentation
  - Architecture
  - API Reference
- Tasks
  - task blocks with assignments
```

### 6.4 Monitor Sync Status

Click the **cloud** sync indicator in the header to open its details ([indicator.cljs:191-245](../../../../../src/main/frontend/components/rtc/indicator.cljs#L191-L245)):

- **Online** / **Offline**
- **pending local changes**
- Asset rows: pending uploads, uploading, downloading, missing files
- **pending server changes**. The source has a `FIXME` on this counter ([indicator.cljs:213-214](../../../../../src/main/frontend/components/rtc/indicator.cljs#L213-L214)), so don't rely on it.
- **Last synced time**, taken from the most recent *upload* log entry ([indicator.cljs:217-220](../../../../../src/main/frontend/components/rtc/indicator.cljs#L217-L220))
- **More debug info**, which shows raw sync state
- **Start sync**, shown when the connection isn't open ([indicator.cljs:238-245](../../../../../src/main/frontend/components/rtc/indicator.cljs#L238-L245))

```txt
Cloud indicator -> details popup
```

## Troubleshooting

### Sync Issues

**Problem**: Changes not appearing for collaborators

**Solutions**:

1. Check your internet connection. The indicator details say **Online** or **Offline**.
2. Open the cloud indicator details and look at **pending local changes**
3. If sync isn't running, click **Start sync** in the details popup
4. Restart Logseq (re-establishes the WebSocket connection)
5. Check that your collaborator's account also has sync access ([Who Can Use Sync](#who-can-use-sync))

### Conflict Storms

**Problem**: Many conflict warnings on the same blocks

**Cause**: Multiple people changing the same blocks at the same time

**Solutions**:

1. **Coordinate**: Use chat to divide work
2. **Work in sections**: Each person focuses on different pages
3. **Take turns**: For complex edits, announce "I'm editing X page"
4. **Review conflicts**: Use **Show sync conflicts** on each affected block, then **Mark resolved**

### Password Issues

**Problem**: Forgot E2EE password

**Solution**:

- ⚠️ **No recovery is possible**, only a destructive reset. Open **Settings → Encryption**, choose **Reset password**, then **Forgot password?**, set a new password, and click **Force reset password** ([settings.cljs:1280-1306](../../../../../src/main/frontend/components/settings.cljs#L1280-L1306), [:1336](../../../../../src/main/frontend/components/settings.cljs#L1336); [en.edn:609](../../../../../src/resources/dicts/en.edn#L609), [:612](../../../../../src/resources/dicts/en.edn#L612), [:619](../../../../../src/resources/dicts/en.edn#L619)). The app warns: "this will make all currently encrypted graph data stored on the server permanently unreadable. After resetting, you'll need to re-upload your graphs from the client." ([en.edn:613](../../../../../src/resources/dicts/en.edn#L613))
- Your **local** copy of the graph stays usable ([en.edn:600](../../../../../src/resources/dicts/en.edn#L600))
- See [E2EE Encryption Guide](../../explanation/security/e2ee-encryption.md#what-happens-if-you-forget) for details

### Account Limits

**Problem**: A notification says **Sync graph count exceed limit** or **Sync storage exceed limit**

**Cause**: The server refused the request for your account ([user.cljs:453-476](../../../../../src/main/frontend/handler/user.cljs#L453-L476), [events/rtc.cljs:61-65](../../../../../src/main/frontend/handler/events/rtc.cljs#L61-L65); [en.edn:1836](../../../../../src/resources/dicts/en.edn#L1836), [:1851](../../../../../src/resources/dicts/en.edn#L1851)). See [Sync Limits](../../reference/features/sync-limits.md).

### Performance Degradation

**Problem**: Laggy typing, slow sync

**Check**:

1. **Indicator details**: a large, growing **pending local changes** count means uploads are not keeping up
2. **Developer console**: check for errors under **View → Toggle Developer Tools** (Electron's standard View menu, [core.cljs:243](../../../../../src/electron/electron/core.cljs#L243))

## What You've Learned

✅ How to create or upload a remote graph with sync enabled
✅ How to invite and remove collaborators, and which roles exist
✅ How to set up encryption passwords
✅ How to read presence and editing indicators
✅ How to find and resolve sync conflicts
✅ How to troubleshoot common issues

## Next Steps

**Try these:**

1. **Invite a friend** and practice simultaneous editing
2. **Create a team wiki** with an organized page structure
3. **Test offline mode**: disconnect, edit, reconnect
4. **Explore related features**:
   - [User profiles](../../reference/features/user-profiles.md)
   - [Asset management](../../how-to/multimedia/asset-management.md) for files
   - [Sync limits](../../reference/features/sync-limits.md)

**Learn more:**

- [E2EE Encryption Deep Dive](../../explanation/security/e2ee-encryption.md)
- [Sync vs. the removed File Sync](../../explanation/comparisons/rtc-vs-file-sync.md)
- [RTC Architecture (Developer Onboarding)](../../../01-onboarding/rtc-onboarding.md)

## Findings: New Capabilities

Current source has these notable capabilities:

1. **Per-entry reject metadata for tx batches**
   - `tx/reject` can return `success-tx-ids`, `failed-tx-id` and `missing-block-uuids`, so the client can reconcile a partially applied batch precisely ([sync.cljs:640-655](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L640-L655))
2. **Checksum-aware sync responses**
   - `hello`, `pull/ok` and `tx/batch/ok` carry a checksum when one is available ([ws.cljs:15-19](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/ws.cljs#L15-L19), [sync.cljs:305-312](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L305-L312), [:637-639](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L637-L639)). The indicator's debug view only displays them in dev/test builds ([indicator.cljs:197](../../../../../src/main/frontend/components/rtc/indicator.cljs#L197)).
3. **Graph readiness gating during bootstrap**
   - While a snapshot is still uploading, the server rejects batches ([sync.cljs:615-618](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L615-L618)), the client won't start sync for a graph marked not ready ([sync.cljs:120-125](../../../../../src/main/frontend/handler/db_based/sync.cljs#L120-L125)), and the graph list shows **Preparing**
4. **Sync conflict popup**
   - Concurrent text edits leave a per-block **Sync conflicts** record with Copy and **Mark resolved** (Step 4.2)
5. **Member-management collaboration flow**
   - Collaboration is built on graph members (`/graphs/:graph-id/members`), with invite and remove operations. There are no share links ([sync.cljs:378-436](../../../../../src/main/frontend/handler/db_based/sync.cljs#L378-L436)).

## Reference: Key Concepts

### Transactions

Every edit generates **Datascript transaction data**, and that data is what gets synchronized:

```clojure
;; Illustrative tx-data
;; [:db/add entity-id :block/title "New content"]
;; [:db/add entity-id :block/updated-at timestamp]
;; [:db/retract entity-id :block/title "Old content"]
```

For each pending transaction, the client encrypts the tx-data if the graph is E2EE, serializes it as Transit, and sends a batch of them in a `tx/batch` message ([apply_txs.cljs:979-997](../../../../../src/main/frontend/worker/sync/apply_txs.cljs#L979-L997)). Encryption applies only to the values of `:block/title` and `:block/name` ([const.cljs:25-27](../../../../../src/main/frontend/worker/sync/const.cljs#L25-L27)). The server assigns each accepted change a new, increasing transaction number `t`.

### Fractional Indexing

Blocks are ordered by the `:block/order` attribute, which uses **fractional indexing** (`logseq.db.common.order`, built on `logseq.clj-fractional-indexing`, [order.cljs:4](../../../../../deps/db/src/logseq/db/common/order.cljs#L4), [:24](../../../../../deps/db/src/logseq/db/common/order.cljs#L24)). A new block can be inserted between any two others without renumbering:

```txt
Block A:  order="a0"
Block B:  order="a2"

Insert between A and B:  order="a1"  (illustrative; generated by generate-key-between)
```

**Advantage**: Two people can insert blocks at different positions without conflict. The sync code also detects and fixes duplicate order keys that concurrent edits can produce (`fix-duplicate-orders!`, [db_sync/order.cljs:5](../../../../../deps/db-sync/src/logseq/db_sync/order.cljs#L5)).

### WebSocket Protocol

Sync uses a **WebSocket**. Messages are JSON objects with a `type` field ([ws.cljs:10-59](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/ws.cljs#L10-L59)):

```txt
Client → Server:   {"type": "hello", "client": "<repo>"}
Client → Server:   {"type": "tx/batch", "t-before": 42, "client-revision": "...", "txs": [{"tx": "<transit>", "tx-id": "<uuid>"}]}
Client → Server:   {"type": "pull", "since": 42}
Client → Server:   {"type": "presence", "editing-block-uuid": "<uuid>"}
Client → Server:   {"type": "ping"}                      (accepted by the server; the app client doesn't send it)

Server → Client:   {"type": "hello", "t": 42, "checksum": "..."}
Server → Client:   {"type": "tx/batch/ok", "t": 43, "checksum": "..."}
Server → Client:   {"type": "tx/reject", "reason": "stale", "t": 43}
Server → Client:   {"type": "tx/reject", "reason": "db transact failed", "t": 43, "success-tx-ids": ["..."], "failed-tx-id": "..."}
Server → Client:   {"type": "changed", "t": 43}
Server → Client:   {"type": "pull/ok", "t": 43, "txs": [...], "checksum": "..."}
Server → Client:   {"type": "online-users", "online-users": [...]}
Server → Client:   {"type": "presence", "user-id": "...", "editing-block-uuid": "..."}
Server → Client:   {"type": "pong"}
Server → Client:   {"type": "error", "message": "unknown type"}
```

Other `tx/reject` reasons are `snapshot upload in progress`, `invalid t-before`, `empty tx data` and `invalid tx` ([sync.cljs:615-658](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs#L615-L658), [ws.cljs:57](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/ws.cljs#L57)).

**Encrypted**: In an E2EE graph, only the `:block/title` / `:block/name` values inside each `tx` are encrypted with the graph's AES key. The rest of the message is plain JSON over TLS.

---

**Last Updated**: 2026-09-11
**Status**: 🟡 Active Development (sync is labelled beta in the app)
