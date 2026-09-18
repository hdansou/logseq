---
title: 'End-to-End Encryption (E2EE) in Logseq'
status: '🟡 Active Development'
version: 'DB graphs with sync; app version 2.0.1 at be800f1711'
quadrant: 'Explanation'
platform: 'Desktop (macOS/Windows/Linux), Mobile (iOS/Android), Web, Node/CLI'
related-docs:
  - '../../tutorials/collaboration/real-time-sync.md'
  - '../../reference/features/user-profiles.md'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
---

# End-to-End Encryption (E2EE) in Logseq

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

> **Last revised:** 2026-09-09

## Overview

Logseq's End-to-End Encryption (E2EE) protects **some** of a synced DB graph's content by encrypting it on your device before it is sent to the sync server. The password that unlocks your key material never reaches Logseq's servers.

This is **selective** encryption, not whole-database encryption. The server still holds the full shape of your graph — every entity, every reference, every property except the two encrypted ones — and can read all of it. Read [What Gets Protected](#what-gets-protected) and [Security Guarantees](#security-guarantees) before relying on this feature for a threat model.

**Key Principles:**

- **Client-side encryption**: all encryption and decryption happens in the app's DB worker, never on the server
- **Password-based key protection**: your password unlocks an RSA private key; the password itself is never uploaded
- **Local password persistence**: the E2EE password is stored locally, encrypted with your OAuth refresh token, in the OS keychain (desktop), native secure storage (mobile), or IndexedDB (web/Node)
- **RSA + AES hybrid**: RSA-OAEP-4096 wraps a per-graph AES-GCM-256 key
- **Per graph, chosen at creation**: encryption is a graph-level flag, on by default, and not changeable afterwards

## Why E2EE Matters

### The Privacy Problem

Traditional cloud storage services can access your data:

```txt
Your Device → Plain Text → Server (Can Read!) → Storage
```

With Logseq's E2EE:

```txt
Your Device → Graph structure in plaintext + encrypted block text/asset bytes → Server → Storage
                    ↓
               Password Key
              (Never Leaves Device)
```

### Is It On?

E2EE is a **per-graph flag chosen when the graph is created**, not a global setting:

- The `Encrypt graph data` checkbox appears only once you tick `Use Logseq Sync?` in the new-graph dialog ([`frontend.components.repo/new-db-graph-inner`](../../../../../src/main/frontend/components/repo.cljs#L713-L723); labels at [`en.edn:745`](../../../../../src/resources/dicts/en.edn#L745) and [`:786`](../../../../../src/resources/dicts/en.edn#L786)).
- It defaults to **on** (`(hooks/use-state true)`, [repo.cljs:646](../../../../../src/main/frontend/components/repo.cljs#L646)), so E2EE is opt-**out**, not opt-in.
- The choice is stored as the KV entity `:logseq.kv/graph-rtc-e2ee?` ([`kv_entity.cljs:32`](../../../../../deps/db/src/logseq/db/frontend/kv_entity.cljs#L32)) and mirrored server-side in the `graphs.graph_e2ee` column, default `1` ([`0002_add_graph_e2ee.sql`](../../../../../deps/db-sync/worker/migrations/0002_add_graph_e2ee.sql)).
- ADR 0003 states the mode is "treated as immutable for that graph after creation" and that no migration flow between modes exists. No code path at `be800f1711` flips the flag for an existing graph.

Local-only graphs (no sync) never involve E2EE at all.

### What Gets Protected

✅ **Encrypted before upload:**

- Block textual content — the `:block/title` attribute
- Page names — the `:block/name` attribute
  (both from `encrypt-attr-set`, [`frontend.worker.sync.const:25-27`](../../../../../src/main/frontend/worker/sync/const.cljs#L25-L27))
- **Asset bytes** — images, PDFs, audio, video are encrypted with the graph AES key before the `PUT` to object storage ([`frontend.worker.sync.assets:167-168`](../../../../../src/main/frontend/worker/sync/assets.cljs#L167-L168), decrypted at [assets.cljs:381](../../../../../src/main/frontend/worker/sync/assets.cljs#L381))
- **Oversized block titles** — a title over 4096 UTF-8 bytes is offloaded to object storage as a separate file and encrypted the same way ([`frontend.worker.sync.large-title:8`](../../../../../src/main/frontend/worker/sync/large_title.cljs#L8) and [:87-90](../../../../../src/main/frontend/worker/sync/large_title.cljs#L87-L90))

❌ **Not encrypted, even for an E2EE graph:**

- Every other block attribute and property value — only two attributes are in `encrypt-attr-set`
- Entity structure: `:block/parent`, `:block/order`, `:block/refs`, `:block/tags`, class and property idents
- Graph name, schema version, creation and update timestamps (`graphs` table, [`0001_init.sql:1-8`](../../../../../deps/db-sync/worker/migrations/0001_init.sql#L1-L8))
- Your account email and username (`users` table, [`0001_init.sql:10-15`](../../../../../deps/db-sync/worker/migrations/0001_init.sql#L10-L15))
- Collaborator membership and roles (`graph_members`, [`0001_init.sql:25-33`](../../../../../deps/db-sync/worker/migrations/0001_init.sql#L25-L33))
- Asset file names on the sync path are UUID + extension, but the *extension* (hence the file type) is visible

❌ **Not encrypted locally:**

- The local SQLite database and IndexedDB — the decrypted graph lives unencrypted on disk
- File-based (non-DB) graphs, which have no sync-time E2EE path at all

## How E2EE Works

### Encryption Architecture

Logseq uses a **hybrid cryptography system**: RSA-OAEP with a 4096-bit modulus for key wrapping, AES-GCM-256 for data:

```txt
┌─────────────────────────────────────────────────────────────────┐
│                     Initial Setup (First Time)                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  1. User sets password                                           │
│     ├─► Generate RSA-OAEP-4096 key pair (public + private)      │
│     └─► Encrypt private key with password (PBKDF2 → AES-GCM)    │
│                                                                   │
│  2. First encrypted graph                                        │
│     ├─► Generate AES-GCM-256 key for this graph                 │
│     └─► Wrap AES key with your RSA public key                   │
│                                                                   │
│  3. Store keys                                                   │
│     ├─► public key + encrypted private key → server             │
│     │   (POST /e2ee/user-keys)                                   │
│     ├─► RSA-wrapped AES key → server                            │
│     │   (POST /e2ee/graphs/:id/aes-key)                          │
│     ├─► Password (encrypted with refresh token) → keychain/     │
│     │   secure storage / IndexedDB                               │
│     └─► Decrypted AES CryptoKey → worker memory only            │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                     Data Flow (Every Edit)                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Local Edit                                                      │
│     │                                                            │
│     ├─► Build tx items [op e a v ...]                           │
│     ├─► For a ∈ #{:block/title :block/name}:                    │
│     │     transit-encode v, AES-GCM encrypt, transit-encode      │
│     │     the [iv ciphertext] pair back into the slot            │
│     ├─► Send tx payload (all other attrs in plaintext)          │
│     └─► Server applies the tx to its own DataScript DB           │
│                                                                   │
│  Remote Update                                                   │
│     │                                                            │
│     ├─► Download tx / snapshot rows from server                 │
│     ├─► Decrypt the two protected attributes                    │
│     └─► Apply into local database                               │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

The encrypt/decrypt walk over tx items is `encrypt-tx-item` / `decrypt-tx-item` ([`frontend.worker.sync.crypt:717-753`](../../../../../src/main/frontend/worker/sync/crypt.cljs#L717-L753)); the snapshot equivalents are `<decrypt-snapshot-rows-batch`, `<decrypt-snapshot-datoms-batch` and `<encrypt-datoms` ([crypt.cljs:785-821](../../../../../src/main/frontend/worker/sync/crypt.cljs#L785-L821)).

### Cryptographic Algorithms

Everything below is Web Crypto (`crypto.subtle`) — see [`frontend.common.crypt`](../../../../../src/main/frontend/common/crypt.cljs), 313 lines.

#### 1. RSA-OAEP (4096-bit)

**Purpose**: wrap the per-graph AES key so it can be stored on the server and shared with collaborators.

**Algorithm Details**:

- Modulus length: 4096 bits
- Public exponent: 65537 (`Uint8Array [1 0 1]`)
- Hash function: SHA-256
- Padding: OAEP
- Public key exported as `spki`, private key as `pkcs8` ([crypt.cljs:30-48](../../../../../src/main/frontend/common/crypt.cljs#L30-L48))

**Implementation** ([`frontend.common.crypt/<generate-rsa-key-pair`](../../../../../src/main/frontend/common/crypt.cljs#L60-L74)):

```clojure
(.generateKey subtle
  #js {:name "RSA-OAEP"
       :modulusLength 4096
       :publicExponent (js/Uint8Array. [1 0 1])
       :hash "SHA-256"}
  true
  #js ["encrypt" "decrypt"])
```

The wrap/unwrap pair is `<encrypt-aes-key` / `<decrypt-aes-key` ([crypt.cljs:164-192](../../../../../src/main/frontend/common/crypt.cljs#L164-L192)): the AES key is exported `raw` and encrypted directly under RSA-OAEP.

#### 2. AES-GCM (256-bit)

**Purpose**: encrypt `:block/title`, `:block/name`, asset bytes and offloaded large titles.

**Algorithm Details**:

- Key size: 256 bits
- Mode: GCM (AEAD — confidentiality plus integrity)
- IV: 12 bytes, freshly generated per encryption via `crypto.getRandomValues`
- The result is carried as a `[iv ciphertext]` pair, transit-encoded ([crypt.cljs:194-231](../../../../../src/main/frontend/common/crypt.cljs#L194-L231))

**Implementation** ([`frontend.common.crypt/<generate-aes-key`](../../../../../src/main/frontend/common/crypt.cljs#L76-L83)):

```clojure
(.generateKey subtle
  #js {:name "AES-GCM"
       :length 256}
  true
  #js ["encrypt" "decrypt"])
```

> **Unverified:** the GCM authentication tag length is not set explicitly anywhere in `crypt.cljs`, so it is whatever the Web Crypto default is for the runtime. This document does not assert a tag length.

#### 3. PBKDF2 (Password-Based Key Derivation)

**Purpose**: derive an AES-GCM key from your password, used to encrypt the RSA private key; and separately to derive a key from your refresh token, used to encrypt the password at rest.

**Algorithm Details**:

- Hash function: SHA-256
- Iterations: **600,000** for the current key schedule
- Salt: 16 bytes from `crypto.getRandomValues`
- IV: 12 bytes
- Derived key: AES-GCM, 256 bits

**Versioned key schedule.** `<encrypt-private-key` stamps a version tag and returns a 4-element vector `["20251210" salt iv ciphertext]` ([crypt.cljs:85-111](../../../../../src/main/frontend/common/crypt.cljs#L85-L111)):

```clojure
(.deriveKey subtle
  #js {:name "PBKDF2"
       :salt salt
       :iterations 600000
       :hash "SHA-256"}
  password-key
  #js {:name "AES-GCM" :length 256}
  true
  #js ["encrypt" "decrypt"])
```

`<decrypt-private-key` accepts both the versioned 4-element form and a legacy 3-element `[salt iv ciphertext]` form, and picks the iteration count accordingly — **600,000 for version ≥ `"20251210"`, 100,000 otherwise** ([crypt.cljs:113-162](../../../../../src/main/frontend/common/crypt.cljs#L113-L162)). A private key encrypted before that bump keeps its weaker 100,000-iteration schedule until it is re-encrypted (which happens on a password change or key reset — see [Password rotation and reset](#password-rotation-and-reset)).

The same PBKDF2 parameters are used by `<encrypt-text-by-text-password` / `<decrypt-text-by-text-password` ([crypt.cljs:257-313](../../../../../src/main/frontend/common/crypt.cljs#L257-L313)), which are what protect the persisted password. That pair is unversioned and always uses 600,000 iterations.

A decryption failure caused by a wrong password surfaces as an `OperationError` and is turned into `ex-info` with `:invalid-password? true` rather than being logged as an error ([crypt.cljs:9-12](../../../../../src/main/frontend/common/crypt.cljs#L9-L12), [:155-162](../../../../../src/main/frontend/common/crypt.cljs#L155-L162)).

### Key Management

#### Password Storage

Your E2EE password **never leaves your device** and **is never sent to the server**. It is persisted locally by encrypting it with your OAuth **refresh token** (PBKDF2-600k → AES-GCM-256) and writing the transit-encoded result under the key `"logseq-encrypted-password"` ([`frontend.worker.sync.crypt:21`](../../../../../src/main/frontend/worker/sync/crypt.cljs#L21), [:113-134](../../../../../src/main/frontend/worker/sync/crypt.cljs#L113-L134)).

> **Removed:** prior wording said web persistence used OPFS for E2EE password storage. The browser platform adapter stores secret text in IndexedDB KV ([`frontend.worker.platform.browser:62-72`](../../../../../src/main/frontend/worker/platform/browser.cljs#L62-L72)).

```txt
User Password
      ↓
Encrypt with refresh token (PBKDF2-600k/SHA-256 + AES-GCM-256)
      ↓
Encrypted password → OS Keychain (Electron) / native secure storage (Capacitor)
                     / IndexedDB KV (browser, Node)
```

Note the consequence: because the wrapping key is derived from the refresh token, losing or rotating the refresh token makes the persisted password undecryptable and you are prompted again. The read path fails fast with `:db-sync/missing-e2ee-password` when there is no refresh token ([crypt.cljs:75-79](../../../../../src/main/frontend/worker/sync/crypt.cljs#L75-L79)).

#### Platform Secret Storage

The worker asks the main thread to use native storage only on Capacitor; on every other platform it writes through the platform adapter ([`frontend.worker.sync.crypt:113-134`](../../../../../src/main/frontend/worker/sync/crypt.cljs#L113-L134)). The main-thread bridge is [`frontend.handler.e2ee`](../../../../../src/main/frontend/handler/e2ee.cljs), which routes to Electron IPC or the Capacitor secure-storage plugin, and returns nil elsewhere ([handler/e2ee.cljs:12-46](../../../../../src/main/frontend/handler/e2ee.cljs#L12-L46)).

**Desktop (Electron)**: [`electron.keychain`](../../../../../src/electron/electron/keychain.cljs) wraps the `keytar` library. The keychain *service* name is the Electron app name plus `" E2EE"` — `"Logseq E2EE"` for the default app name ([keychain.cljs:9-19](../../../../../src/electron/electron/keychain.cljs#L9-L19)) — and the *account* is the key `"logseq-encrypted-password"`.

**Implementation** ([`electron.keychain/<set-password!`](../../../../../src/electron/electron/keychain.cljs#L25-L34)):

```clojure
(defn <set-password!
  "Persist `encrypted-text` for the `refresh-token` entry."
  [key encrypted-text]
  (if-let [account (and (supported?) key)]
    (-> (p/let [_ (.setPassword keytar (keychain-service) account encrypted-text)]
          true)
        (p/catch (fn [e]
                   (logger/error ::set-password {:error e})
                   (throw e))))
    (p/resolved false)))
```

> **Unverified:** which OS credential store `keytar` ends up using on each platform (macOS Keychain Services, Windows Credential Manager/DPAPI, Linux libsecret) is a property of the `keytar` package, not of this repository. Earlier revisions of this page asserted those backends and their protection guarantees; that has been removed because it cannot be verified from Logseq source.

**Mobile (Capacitor)**: `frontend.mobile.secure-storage`, reached via [`frontend.handler.e2ee:18-19`](../../../../../src/main/frontend/handler/e2ee.cljs#L18-L19).

**Web and Node**: IndexedDB KV — no OS-level protection.

#### Per-Graph AES Key

`<ensure-graph-aes-key` ([`frontend.worker.sync.crypt:578-624`](../../../../../src/main/frontend/worker/sync/crypt.cljs#L578-L624)) resolves the key for a graph in this order:

1. In-memory cache `*graph->aes-key` ([crypt.cljs:17](../../../../../src/main/frontend/worker/sync/crypt.cljs#L17))
2. Locally cached wrapped key at IndexedDB key `rtc-encrypted-aes-key###<graph-id>` ([crypt.cljs:252-254](../../../../../src/main/frontend/worker/sync/crypt.cljs#L252-L254))
3. Server: `GET /e2ee/graphs/:graph-id/aes-key` ([crypt.cljs:501-505](../../../../../src/main/frontend/worker/sync/crypt.cljs#L501-L505))
4. If none exists, generate a fresh AES key, wrap it with your own RSA public key and `POST` it ([crypt.cljs:615-619](../../../../../src/main/frontend/worker/sync/crypt.cljs#L615-L619))

If the locally cached wrapped key fails to unwrap, the cache is cleared and the server copy refetched once before giving up ([crypt.cljs:596-614](../../../../../src/main/frontend/worker/sync/crypt.cljs#L596-L614)). The user RSA key pair is cached the same way and invalidated on the same class of failure ([crypt.cljs:526-562](../../../../../src/main/frontend/worker/sync/crypt.cljs#L526-L562)).

### Server Endpoints

| Endpoint | Method | Purpose | Source |
| --- | --- | --- | --- |
| `/e2ee/user-keys` | GET | fetch your public key + encrypted private key | [crypt.cljs:260-264](../../../../../src/main/frontend/worker/sync/crypt.cljs#L260-L264) |
| `/e2ee/user-keys` | POST | upload/replace them | [crypt.cljs:317-331](../../../../../src/main/frontend/worker/sync/crypt.cljs#L317-L331) |
| `/e2ee/user-public-key?email=` | GET | look up a collaborator's public key | [crypt.cljs:495-499](../../../../../src/main/frontend/worker/sync/crypt.cljs#L495-L499) |
| `/e2ee/graphs/:id/aes-key` | GET / POST | fetch or upsert your wrapped copy of the graph AES key | [crypt.cljs:501-524](../../../../../src/main/frontend/worker/sync/crypt.cljs#L501-L524) |
| `/e2ee/graphs/:id/grant-access` | POST | hand a collaborator the AES key wrapped under their public key | [crypt.cljs:689-693](../../../../../src/main/frontend/worker/sync/crypt.cljs#L689-L693) |

Request/response shapes are pinned by Malli schemas in [`logseq.db-sync.malli-schema:197-230`](../../../../../deps/db-sync/src/logseq/db_sync/malli_schema.cljs#L197-L230).

### First-Time Setup Flow

When you create an encrypted graph and have no key pair yet:

```txt
Step 1: Password Entry UI  ("Set password for remote graphs")
┌──────────────────────────────────────────────┐
│  Password:     [****************]  👁        │
│  Confirm:      [****************]  👁        │
│                                              │
│  ⚠️  Remember this password — we cannot      │
│      reset or retrieve it.                   │
│  ⚠️  Keep a secure backup.                   │
│                                              │
│  [Submit]                                    │
└──────────────────────────────────────────────┘

Step 2: Key Generation (client-side, in the DB worker)
├─► Generate RSA-OAEP-4096 key pair
├─► Derive AES-GCM key from password (PBKDF2-600k/SHA-256, 16-byte salt)
└─► Encrypt exported pkcs8 private key with it → ["20251210" salt iv ct]

Step 3: Storage
├─► POST public key + encrypted private key to /e2ee/user-keys
├─► Encrypt password with refresh token, persist under
│   "logseq-encrypted-password"
└─► Cache the RSA pair in IndexedDB, keyed by server base + user id

Step 4: Graph AES key
├─► Generate AES-GCM-256 key
├─► Wrap under your RSA public key, POST to /e2ee/graphs/:id/aes-key
└─► Cache wrapped key locally; keep the CryptoKey in worker memory
```

Steps 2–3 are `<generate-and-upload-user-rsa-key-pair!` ([crypt.cljs:378-399](../../../../../src/main/frontend/worker/sync/crypt.cljs#L378-L399)); the submit button in the new-graph dialog stays disabled until the key pair exists ([repo.cljs:725](../../../../../src/main/frontend/components/repo.cljs#L725)).

**UI Component** ([`frontend.components.e2ee/e2ee-request-new-password`](../../../../../src/main/frontend/components/e2ee.cljs#L13-L58)):

- Password confirmation to prevent typos; `Submit` disabled while blank or mismatched
- Warning copy about the password being unrecoverable
- **No password strength meter and no minimum length** — any non-blank string is accepted

### Decryption Flow (Subsequent Sessions)

```txt
Step 1: Try the persisted password
├─► Read "logseq-encrypted-password", decrypt with refresh token
└─► Success → skip the prompt entirely

Step 2: Password Prompt  ("Enter password for remote graphs")
┌──────────────────────────────────────────────┐
│  Password:     [****************]  👁        │
│  [Submit]                                    │
└──────────────────────────────────────────────┘

Step 3: Key Recovery
├─► Fetch encrypted private key (cache, else GET /e2ee/user-keys)
├─► Decrypt with the password (PBKDF2 + AES-GCM)
│   ├─► Success → persist the password locally, continue
│   └─► OperationError → "Wrong password"
└─► Unwrap the graph AES key with the private key

Step 4: Sync Data Decryption
├─► Download tx / snapshot rows
├─► Decrypt :block/title and :block/name values
├─► Decrypt asset bytes on asset download
└─► Apply into local database
```

The headless-first, prompt-second ordering is `<decrypt-private-key` ([crypt.cljs:452-488](../../../../../src/main/frontend/worker/sync/crypt.cljs#L452-L488)); a non-interactive runtime (Node without Electron) never prompts and fails with `:db-sync/missing-e2ee-password` instead.

**UI Component** ([`frontend.components.e2ee/e2ee-password-to-decrypt-private-key`](../../../../../src/main/frontend/components/e2ee.cljs#L89-L126)):

- Single password field
- `"Wrong password"` shown when `<decrypt-private-key` rejects
- Enter key submits
- On success, calls `:thread-api/save-e2ee-password` to persist the password for next time ([components/e2ee.cljs:99](../../../../../src/main/frontend/components/e2ee.cljs#L99))

A third, simpler modal `e2ee-request-password` ([components/e2ee.cljs:60-87](../../../../../src/main/frontend/components/e2ee.cljs#L60-L87)) is used where the worker just needs a password string and verifies it itself.

### Data Encryption During Sync

```txt
Local Edit → tx items [op e a v ...]
            ↓
      a ∈ #{:block/title :block/name}?
            ↓ yes
      transit-encode v
            ↓
      Generate random 12-byte IV
            ↓
      AES-GCM encrypt → [iv ciphertext]
            ↓
      transit-encode the pair back into slot 3
            ↓
      WebSocket/HTTP → Server
            ↓
      Server applies the tx to its own DataScript DB
      (structure plaintext, those two values ciphertext)
            ↓
      Broadcast to Other Clients
            ↓
      Decrypt with graph AES key
            ↓
      Apply to Local Database
```

**Security Properties:**

- **IV uniqueness**: a fresh random 12-byte IV per encryption operation
- **AEAD**: GCM gives confidentiality and integrity for each encrypted value
- **Graceful degradation on read**: `<decrypt-text-value` returns the raw value unchanged if it is not a valid encrypted package ([crypt.cljs:702-715](../../../../../src/main/frontend/worker/sync/crypt.cljs#L702-L715)) — convenient for mixed data, but it means a stripped-encryption value is accepted silently rather than rejected

> **Removed:** the previous statement "RTC operation timestamps prevent replay attacks" was removed; no replay-protection mechanism tied to E2EE exists in source.

One structural consequence worth knowing: for an E2EE graph, `:block/title` and `:block/name` are **excluded from the sync checksum** ([`logseq.db-sync.checksum/relevant-attrs:71-73`](../../../../../deps/db-sync/src/logseq/db_sync/checksum.cljs#L71-L73)), because ciphertext differs per device. Consistency checking therefore covers graph structure but not encrypted text content.

## Security Guarantees

### What the Server Actually Stores

This is the load-bearing section. Two stores are involved.

**1. The D1 index database** ([`0001_init.sql`](../../../../../deps/db-sync/worker/migrations/0001_init.sql), [`0002_add_graph_e2ee.sql`](../../../../../deps/db-sync/worker/migrations/0002_add_graph_e2ee.sql)):

| Table | Columns | Encrypted? |
| --- | --- | --- |
| `graphs` | `graph_id, graph_name, user_id, schema_version, created_at, updated_at, graph_e2ee` | **No** — the graph name is plaintext |
| `users` | `id, email, email_verified, username` | **No** |
| `user_rsa_keys` | `user_id, public_key, encrypted_private_key` | private key yes, public key no |
| `graph_members` | `user_id, graph_id, role, invited_by` | **No** |
| `graph_aes_keys` | `graph_id, user_id, encrypted_aes_key` | AES key wrapped under that user's RSA public key |

**2. The per-graph Durable Object SQLite database** ([`logseq.db-sync.storage/init-schema!:33-46`](../../../../../deps/db-sync/src/logseq/db_sync/storage.cljs#L33-L46)): tables `kvs`, `tx_log`, `sync_meta`. This is a full DataScript-backed materialisation of your graph on the server. Every datom is there. Only the values of `:block/title` and `:block/name` are ciphertext.

So the accurate statement is: **the server cannot read your block text, page names or asset bytes, but it can read the entire structure of your graph** — how many blocks exist, how they nest, what refers to what, which tags and classes and properties are used and their values, who the collaborators are, and the graph's name.

### What E2EE Protects Against

✅ **Server compromise, for the encrypted fields**

- Block text, page names and asset bytes stay ciphertext without the AES key
- An attacker with full server access also gets the graph structure, graph names, member emails, and everyone's wrapped keys

✅ **Network eavesdropping**

- TLS in transport, plus the encrypted fields are already ciphertext at the application layer

✅ **Cloud storage leaks**

- Object storage holds only ciphertext for assets and offloaded large titles

✅ **Logseq staff reading your prose**

- Neither the password nor the unwrapped private key is ever uploaded; staff cannot recover block text or asset content

### What E2EE Does NOT Protect Against

❌ **Graph structure and metadata disclosure**

- See the table above. This is the largest gap and it is by design at this revision.

❌ **Local device compromise**

- The local database is not encrypted; keys sit in worker memory; the persisted password is only as protected as the platform store (and on web/Node that is plain IndexedDB)

❌ **Weak passwords**

- PBKDF2-600k slows offline attack on a stolen `encrypted_private_key`, but the UI enforces no strength or length requirement at all

❌ **Phishing**

- A fake client or site that collects your E2EE password gets everything

❌ **Physical access to an unlocked device**

❌ **A malicious or compromised client build**

- The client holds the keys; a compromised build can exfiltrate them

## Password Best Practices

### Creating a Strong Password

Nothing in the app checks this, so it is entirely on you.

**Recommended Approach**: a high-entropy passphrase

✅ **Good Examples**:

- `correct-horse-battery-staple-7821` (Diceware method)
- `T8$mK9@pL2#nQ5&vR3!bW6^xY4*hZ7` (random generator)

❌ **Bad Examples**:

- `password123`, `logseq2024`, `qwerty`

**Minimum Recommendation**:

- **20+ characters**, mixed classes, not reused, not derived from personal information

### Storing Your Password Securely

**Option 1: Password Manager** (recommended) — 1Password, Bitwarden, KeePassXC.

**Option 2: Physical Backup** — written down, stored in a safe.

**Option 3: Memory Only** (high risk) — there is no recovery path.

### What Happens If You Forget

⚠️ There is **no password recovery**. There is a **destructive reset**.

Settings exposes a "forgot password" flow that calls `:thread-api/reset-user-rsa-key-pair` ([`frontend.components.settings:1288`](../../../../../src/main/frontend/components/settings.cljs#L1300)). The app's own warning text states the consequence exactly ([`en.edn:613`](../../../../../src/resources/dicts/en.edn#L613)):

> "If you forget your password, you can force a reset of your encryption password. However, this will make all currently encrypted graph data stored on the server permanently unreadable. After resetting, you'll need to re-upload your graphs from the client."

That is because the reset generates a **new** RSA key pair ([crypt.cljs:875-887](../../../../../src/main/frontend/worker/sync/crypt.cljs#L875-L887)); every existing `graph_aes_keys` row is wrapped under the **old** public key and can no longer be unwrapped.

**Consequences:**

1. Server-side encrypted content becomes permanently unreadable
2. Logseq support cannot help — they never had the password

**Mitigation** (also stated in-app, [`en.edn:600`](../../../../../src/resources/dicts/en.edn#L600)):

1. The **local** copy of the graph is unencrypted and still fully usable
2. After a reset you re-upload the graph from a client that still has the local data

### Password Rotation and Reset

Two distinct operations exist:

- **Change password** — `:thread-api/change-e2ee-password` ([crypt.cljs:889-891](../../../../../src/main/frontend/worker/sync/crypt.cljs#L889-L891), UI at [settings.cljs:1401](../../../../../src/main/frontend/components/settings.cljs#L1401)). Decrypts the private key with the old password, re-encrypts it with the new one (picking up the current 600,000-iteration schedule), uploads it, and re-persists the local secret. Graph AES keys and graph content are **not** touched, so nothing is re-encrypted ([crypt.cljs:823-841](../../../../../src/main/frontend/worker/sync/crypt.cljs#L823-L841)).
- **Reset key pair** — `:thread-api/reset-user-rsa-key-pair`, the destructive path described above.

Neither rotates the per-graph AES key. There is no AES key rotation in source at this revision.

## Implementation Details

### Cryptography Libraries

Logseq uses the **Web Crypto API** exclusively (`(.. js/crypto -subtle)`, [crypt.cljs:7](../../../../../src/main/frontend/common/crypt.cljs#L7)). There is no libsodium, tweetnacl or other bundled crypto library in the tree.

**Advantages:**

- **Native**: implemented in the browser/runtime
- **Standards-based**: W3C specification
- **Hardware acceleration** for AES where the platform provides it

**Electron**: uses Chromium's implementation, plus `keytar` for OS keychain access (for the encrypted password only — no key material goes into the keychain).

### Key Storage Locations

**Server-side**:

- `user_rsa_keys.public_key` (plaintext) and `user_rsa_keys.encrypted_private_key` (password-encrypted), via `/e2ee/user-keys`
- `graph_aes_keys.encrypted_aes_key` per (graph, user), via `/e2ee/graphs/:id/aes-key`

**Client-side (Desktop)**:

```txt
OS keychain (via keytar) stores: E2EE password encrypted with refresh token
  Service: "<app name> E2EE"  → "Logseq E2EE" by default
  Account: "logseq-encrypted-password"
```

**Client-side (Mobile)**:

```txt
Capacitor secure storage
  Key:   "logseq-encrypted-password"
  Value: E2EE password encrypted with refresh token
```

**Client-side (Web / Node)**:

```txt
IndexedDB KV (browser platform adapter)
  "logseq-encrypted-password"                 → password encrypted with refresh token
  "rtc-encrypted-aes-key###<graph-id>"        → RSA-wrapped graph AES key
  "rtc-user-rsa-key-pair###<base>###<user-id>" → cached {public-key, encrypted-private-key}
```

**In-memory (all platforms)**:

```clojure
;; Decrypted AES CryptoKey per graph, in the DB worker
;; atom: *graph->aes-key {graph-id CryptoKey}
```

### Code References

**Core cryptographic primitives** — [`frontend.common.crypt`](../../../../../src/main/frontend/common/crypt.cljs) (313 lines):

- `<generate-rsa-key-pair` / `<generate-aes-key`
- `<export-public-key` / `<import-public-key` (spki), `<export-private-key` (pkcs8)
- `<encrypt-private-key` / `<decrypt-private-key` — PBKDF2 + AES-GCM, versioned
- `<encrypt-aes-key` / `<decrypt-aes-key` — RSA-OAEP wrap/unwrap
- `<encrypt-uint8array` / `<decrypt-uint8array` — binary (assets)
- `<encrypt-text` / `<decrypt-text` / `<decrypt-text-if-encrypted` — transit-encoded text
- `<encrypt-text-by-text-password` / `<decrypt-text-by-text-password` — password-at-rest

**Sync-layer E2EE logic** — [`frontend.worker.sync.crypt`](../../../../../src/main/frontend/worker/sync/crypt.cljs) (909 lines):

- `<ensure-graph-aes-key`, `<fetch-graph-aes-key-for-download`
- `<encrypt-tx-data` / `<decrypt-tx-data`, `<encrypt-datoms`, snapshot decrypt helpers
- `<grant-graph-access!`
- `<change-e2ee-password!`, and the `def-thread-api` entry points at [:847-909](../../../../../src/main/frontend/worker/sync/crypt.cljs#L847-L909)

**Asset encryption** — [`frontend.worker.sync.assets`](../../../../../src/main/frontend/worker/sync/assets.cljs) and [`frontend.worker.sync.large-title`](../../../../../src/main/frontend/worker/sync/large_title.cljs)

**UI Components** — [`frontend.components.e2ee`](../../../../../src/main/frontend/components/e2ee.cljs) (126 lines): three modals, listed above.

**Main-thread bridge** — [`frontend.handler.e2ee`](../../../../../src/main/frontend/handler/e2ee.cljs) (68 lines): `<native-save-secret!` / `<native-get-secret` / `<native-delete-secret!`, backed by private `<keychain-*` fns.

**Keychain (Electron)** — [`electron.keychain`](../../../../../src/electron/electron/keychain.cljs) (55 lines): `<set-password!`, `<get-password`, `<delete-password!`.

**Encrypted attribute set** — [`frontend.worker.sync.const/encrypt-attr-set`](../../../../../src/main/frontend/worker/sync/const.cljs#L25-L27): `#{:block/title :block/name}`.

**Tests as ground truth** — [`frontend.common.crypt-test`](../../../../../src/test/frontend/common/crypt_test.cljs) covers the full generate → encrypt → decrypt round trip and the wrong-password/wrong-private-key rejection paths; [`frontend.worker.sync.crypt-test`](../../../../../src/test/frontend/worker/sync/crypt_test.cljs) covers per-runtime secret storage, prompt fallback and cache invalidation.

## Where the ADR and the Code Disagree

[ADR 0003](../../../../../docs/adr/0003-optional-sync-graph-encryption.md) is still marked **Status: Proposed** (dated 2026-02-11), but the decision it describes is implemented: the graph-creation flag exists, defaults to on, is persisted as `:logseq.kv/graph-rtc-e2ee?` and as `graphs.graph_e2ee`, and both sync and asset paths branch on it. The ADR status is stale relative to the code.

Two ADR statements are worth reading carefully against the code:

- The ADR says the mode is "treated as immutable for that graph after creation", and no code changes it afterwards — but `checksum.cljs` explicitly handles the case where the flag differs between two DB versions and forces a full recompute ([checksum.cljs:407-411](../../../../../deps/db-sync/src/logseq/db_sync/checksum.cljs#L407-L411)). The invariant is asserted by convention, not enforced.
- The ADR frames the choice as "encrypted graph data" versus plaintext. In the implemented version, "encrypted" covers two block attributes plus asset bytes, not the graph as a whole. The ADR's framing overstates what the encrypted mode does.

## Frequently Asked Questions

### General Questions

**Q: Is E2EE enabled by default?**

A: For a new **sync** graph, yes — the `Encrypt graph data` checkbox is pre-ticked. It is opt-out at creation time. Local-only graphs have no E2EE.

**Q: Can I turn E2EE on or off for an existing graph?**

A: No. There is no code path at this revision that changes the flag after creation, and the ADR explicitly defers a migration flow.

**Q: Can I use E2EE with local-only graphs?**

A: No. E2EE exists only on the sync path. Local graphs are stored unencrypted.

**Q: Does E2EE slow down Logseq?**

A: RSA-4096 key generation is the one visibly slow step, and it happens once per account. Per-edit AES-GCM on two attributes is negligible. Initial upload encrypts datoms in batches of 5000 with progress reporting ([crypt.cljs:799-821](../../../../../src/main/frontend/worker/sync/crypt.cljs#L799-L821)), so large graphs report progress rather than blocking silently.

**Q: Can I share an E2EE graph with others?**

A: Yes. `<grant-graph-access!` looks up the collaborator's public key by email, wraps the graph AES key under it, and POSTs it to `/e2ee/graphs/:id/grant-access` ([crypt.cljs:666-694](../../../../../src/main/frontend/worker/sync/crypt.cljs#L666-L694)). Each user has their own password and their own RSA pair; all of them end up holding the same per-graph AES key.

> **Unverified:** there is no revoke-key operation in `worker/sync/crypt.cljs`. Removing a member (`<rtc-remove-member!`) drops their membership, but this document cannot confirm from source whether their `graph_aes_keys` row is deleted server-side, and the AES key is not rotated either way.

### Technical Questions

**Q: What happens if I change my password?**

A: The RSA private key is decrypted with the old password and re-encrypted with the new one; the updated blob is uploaded and the local secret is rewritten. Graph AES keys and graph content are untouched.

**Q: Are assets (images, PDFs) encrypted?**

A: **Yes**, for an E2EE graph. Asset bytes are encrypted with the graph AES key before upload and decrypted after download. (Earlier revisions of this page said assets were not encrypted; that was wrong.)

**Q: What about very long blocks?**

A: A `:block/title` over 4096 UTF-8 bytes is offloaded to object storage as a separate `.txt` object, encrypted with the same graph AES key, and the datom carries a `{:asset-uuid, :asset-type}` pointer instead.

**Q: How does E2EE work with Real-Time Collaboration?**

A: The encryption is applied to tx items in the sync worker before transport, and reversed on receipt. The server applies the tx to its own DataScript DB with the two protected values still ciphertext, then broadcasts.

See [Real-Time Sync Tutorial](../../tutorials/collaboration/real-time-sync.md) for details.

**Q: Can the Logseq team see my E2EE password during login?**

A: No. Account authentication (Cognito/JWT) and the E2EE password are separate; the E2EE password is only ever used locally as PBKDF2 input.

**Q: Does E2EE protect against supply chain attacks?**

A: Only partially. Already-uploaded ciphertext stays encrypted, but a compromised client build holds the keys and the password and can exfiltrate both.

### Troubleshooting

**Q: "Wrong password" but I'm sure it's right**

The error appears when `<decrypt-private-key` raises an `OperationError`. Check:

1. Caps Lock / keyboard layout
2. Whitespace pasted around the password
3. Whether someone used the "forgot password" reset, which replaced the RSA key pair — your old password no longer matches the new `encrypted_private_key`

**Q: I'm prompted for the password on every launch**

The persisted password is encrypted with your refresh token. If the refresh token changed (re-login, revoked session), the stored blob no longer decrypts and you will be prompted again.

**Q: Sync fails with `missing-e2ee-password` in Node/CLI**

Non-interactive runtimes never prompt. Supply the password so it can be persisted, then it is read back from secret storage.

**Q: E2EE setup keeps failing**

1. The submit button in the new-graph dialog is disabled until the RSA key pair exists — it needs to reach the sync server
2. RSA-4096 generation needs a working Web Crypto implementation; extensions that shim `crypto.subtle` will break it

## Known Gaps

These are gaps visible in source at `be800f1711`, not a roadmap:

- Only `:block/title` and `:block/name` are in `encrypt-attr-set`; all other property values sync in plaintext
- Graph names, member emails and full graph structure are plaintext server-side
- No AES key rotation; no revocation of a previously shared AES key
- No key export/backup; the only "recovery" is a destructive RSA key-pair reset
- No password strength enforcement in the UI
- Private keys encrypted before the `"20251210"` schedule keep 100,000 PBKDF2 iterations until re-encrypted
- Legacy 3-element encrypted-private-key blobs are still accepted

> **Removed:** the previous "Future Improvements", "Planned Features" and "Research Areas" sections (export/import of encrypted backups, hardware security keys, biometric unlock, metadata encryption, post-quantum cryptography, homomorphic encryption, threshold cryptography) were removed. None of them is backed by anything in the repository, and a security document should not present speculation as a plan.

> **Removed:** the previous comparison table against Notion, Obsidian Sync and Standard Notes was removed. Those columns cannot be verified from Logseq source, and several entries were wrong.

## Conclusion

Logseq's E2EE uses sound, standard primitives — RSA-OAEP-4096 wrapping AES-GCM-256, with PBKDF2-SHA256 at 600,000 iterations guarding the private key — implemented entirely on Web Crypto with no home-rolled cryptography.

What it does **not** do is hide your graph from the server. It encrypts block text, page names, asset bytes and offloaded long titles. Everything else — structure, references, tags, other property values, graph name, collaborators — is stored in plaintext in a server-side DataScript database. Judge the feature against that description, not against the phrase "end-to-end encrypted".

**Remember**: E2EE is only as strong as your password, and nothing in the app checks that your password is strong. There is no recovery — only a reset that destroys server-side encrypted data.

For collaboration features using E2EE, see the [Real-Time Sync Tutorial](../../tutorials/collaboration/real-time-sync.md).

For user profile configuration, see [User Profiles Reference](../../reference/features/user-profiles.md).

---

**Last Updated**: 2026-09-09 (verified against `be800f1711`)
**Status**: 🟡 Active Development (E2EE feature evolving with RTC improvements)
**Feedback**: Report E2EE issues on [GitHub](https://github.com/logseq/logseq/issues) with `encryption` label
