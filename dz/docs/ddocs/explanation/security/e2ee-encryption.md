---
title: 'End-to-End Encryption (E2EE) in Logseq'
status: '🟡 Active Development'
version: 'Since v0.9.0'
quadrant: 'Explanation'
platform: 'Desktop (macOS/Windows/Linux), Mobile (iOS/Android)'
related-docs:
  - '../../tutorials/collaboration/real-time-sync.md'
  - '../../reference/features/user-profiles.md'
---

# End-to-End Encryption (E2EE) in Logseq

> **Last revised:** 2026-05-30

## Overview

Logseq's End-to-End Encryption (E2EE) protects remote graph content by encrypting selected sync attributes before they leave your device. Only you can decrypt protected content using a password that never reaches Logseq servers. This architecture ensures the server cannot decrypt protected fields without your local key material.

**Key Principles:**

- **Zero-knowledge architecture**: Server never sees unencrypted data or passwords
- **Client-side encryption**: All encryption/decryption happens in your browser/app
- **Password-based protection**: You control access with a single master password
- **Secure password storage**: E2EE password persisted in OS keychain (desktop) or secure storage (mobile)
- **RSA + AES hybrid**: Strong cryptography combining asymmetric and symmetric encryption

## Why E2EE Matters

### The Privacy Problem

Traditional cloud storage services can access your data:

```txt
Your Device → Plain Text → Server (Can Read!) → Storage
```

With E2EE:

```txt
Your Device → Encrypted Data → Server (Cannot Read) → Storage
                    ↓
               Password Key
              (Never Leaves Device)
```

### What Gets Protected

✅ **Encrypted in cloud:**

- Block textual content (`:block/title` attribute)
- Page names (`:block/name` attribute)

❌ **Not encrypted (even in cloud):**

- Other block properties and metadata
- Assets (images, PDFs, files)
- Tags and relationships
- Graph-level metadata (graph names, timestamps)

❌ **Not encrypted locally:**

- Local-only graphs (file-based)
- Local cache and IndexedDB
- Electron app storage

## How E2EE Works

### Encryption Architecture

Logseq uses a **hybrid cryptography system** combining RSA-4096 (asymmetric) and AES-256-GCM (symmetric) encryption:

```txt
┌─────────────────────────────────────────────────────────────────┐
│                     Initial Setup (First Time)                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  1. User sets password                                           │
│     ├─► Generate RSA-4096 key pair (public + private)           │
│     ├─► Generate AES-256 key (for data encryption)              │
│     └─► Encrypt private key with password (PBKDF2)              │
│                                                                   │
│  2. Store keys securely                                          │
│     ├─► Encrypted private key + public key → Cloud server       │
│     ├─► Password (encrypted with refresh token) → OS Keychain   │
│     └─► AES key → Memory only (regenerated or shared)           │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                     Data Flow (Every Edit)                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  Local Edit                                                      │
│     │                                                            │
│     ├─► Serialize to Transit format                             │
│     ├─► Encrypt selected attributes with AES-256-GCM            │
│     ├─► Send sync payload with encrypted protected fields        │
│     └─► Server stores payload (cannot decrypt protected fields)  │
│                                                                   │
│  Remote Update                                                   │
│     │                                                            │
│     ├─► Download sync payload from server                       │
│     ├─► Decrypt protected fields with AES-256 key               │
│     ├─► Parse Transit format                                    │
│     └─► Apply changes to local database                         │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

### Cryptographic Algorithms

#### 1. RSA-OAEP (4096-bit)

**Purpose**: Encrypt the AES key for sharing between devices/users

**Algorithm Details**:

- Modulus length: 4096 bits (very strong)
- Public exponent: 65537 (standard)
- Hash function: SHA-256
- Padding: OAEP (Optimal Asymmetric Encryption Padding)

**Implementation** ([`frontend.common.crypt/generate-rsa-key-pair`](../../src/main/frontend/common/crypt.cljs#L63-L77)):

```clojure
(.generateKey subtle
  #js {:name "RSA-OAEP"
       :modulusLength 4096
       :publicExponent (js/Uint8Array. [1 0 1])
       :hash "SHA-256"}
  true
  #js ["encrypt" "decrypt"])
```

#### 2. AES-GCM (256-bit)

**Purpose**: Encrypt selected sync attributes (`:block/title` and `:block/name`)

**Algorithm Details**:

- Key size: 256 bits
- Mode: GCM (Galois/Counter Mode)
- Authentication: Built-in AEAD (Authenticated Encryption with Associated Data)
- IV: 12 bytes (96 bits), randomly generated per encryption

**Implementation** ([`frontend.common.crypt/generate-aes-key`](../../src/main/frontend/common/crypt.cljs#L79-L86)):

```clojure
(.generateKey subtle
  #js {:name "AES-GCM"
       :length 256}
  true
  #js ["encrypt" "decrypt"])
```

#### 3. PBKDF2 (Password-Based Key Derivation)

**Purpose**: Derive encryption key from user's password

**Algorithm Details**:

- Hash function: SHA-256
- Iterations: 600,000 (as of version 20251210)
  - Previous versions: 100,000 iterations
  - High iteration count defends against brute-force attacks
- Salt: 16 bytes (128 bits), randomly generated
- Derived key length: 256 bits (for AES-256)

**Implementation** ([`frontend.common.crypt/encrypt-private-key`](../../src/main/frontend/common/crypt.cljs#L88-L114)):

```clojure
(.deriveKey subtle
  #js {:name "PBKDF2"
       :salt salt
       :iterations 600000  ; Strong protection
       :hash "SHA-256"}
  password-key
  #js {:name "AES-GCM" :length 256}
  true
  #js ["encrypt" "decrypt"])
```

### Key Management

#### Password Storage

Your master password **never leaves your device** and **is never sent to the server**. It is persisted locally by encrypting it with your refresh token and storing the result in OS keychain (desktop), native secure storage (Capacitor), or worker secret storage fallback in web runtime.

> **Removed:** Prior wording said web persistence used OPFS for E2EE password storage. Current worker implementation stores secret text via platform crypto storage backed by IndexedDB KV in browser runtime.

```txt
User Password
      ↓
Encrypt with refresh token (PBKDF2 + AES-GCM)
      ↓
Encrypted password → OS Keychain / Secure Storage / Browser IndexedDB secret storage
```

#### OS Keychain Integration

Logseq uses the operating system's secure keychain to store the encrypted E2EE password ([`electron.keychain`](../../src/electron/electron/keychain.cljs)). The account key used is `"logseq-encrypted-password"`.

**macOS**: Keychain Access

- Service name: `"Logseq E2EE"` (derived from app name)
- Uses Apple's Keychain Services API via `keytar` library
- Protected by system-level security (FileVault, Touch ID, etc.)

**Windows**: Credential Manager

- Uses Windows Credential Locker
- Protected by Windows Data Protection API (DPAPI)

**Linux**: Secret Service API

- Uses `libsecret` (GNOME Keyring, KDE Wallet, etc.)
- Requires desktop environment support

**Mobile (iOS/Android)**: Secure Storage

- Uses Capacitor secure storage plugin
- Protected by platform-level secure enclave / keystore

**Implementation** ([`electron.keychain/set-password!`](../../src/electron/electron/keychain.cljs#L25-L34)):

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

### First-Time Setup Flow

When you enable E2EE for a graph, this sequence occurs:

```txt
Step 1: Password Entry UI
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

Step 2: Key Generation (Client-Side)
├─► Generate RSA-4096 key pair (takes 2-5 seconds)
├─► Generate AES-256 key
├─► Derive key from password using PBKDF2 (600k iterations)
└─► Encrypt private key with derived key

Step 3: Secure Storage
├─► Upload encrypted private key + public key to server
│   (via POST /e2ee/user-keys)
│
├─► Encrypt password with refresh token, store in OS keychain
│   (Account: "logseq-encrypted-password")
│
└─► Cache encrypted AES key and decrypted AES CryptoKey locally for reuse

Step 4: Graph Upload
├─► Serialize graph data to Transit format
├─► Encrypt protected attributes with AES-256-GCM (random IV)
├─► Upload sync payload to cloud
└─► Server stores encrypted protected fields plus plaintext metadata
```

**UI Component** ([`frontend.components.e2ee/e2ee-request-new-password`](../../src/main/frontend/components/e2ee.cljs#L12-L64)):

This modal enforces strong security practices:

- Password confirmation to prevent typos
- Visual warnings about password importance
- Cannot submit until passwords match
- No password strength meter

### Decryption Flow (Subsequent Sessions)

When you open an encrypted graph after restarting:

```txt
Step 1: Password Prompt
┌──────────────────────────────────────────────┐
│  Enter password for remote graphs            │
│                                              │
│  Password:     [****************]  👁        │
│                                              │
│  [Submit]                                    │
└──────────────────────────────────────────────┘

Step 2: Key Recovery
├─► Retrieve encrypted private key from server (/e2ee/user-keys)
├─► Decrypt private key using entered password (PBKDF2 + AES-GCM)
│   ├─► Success → Proceed to Step 3
│   └─► Failure → Show "Wrong password" error
│
└─► Persist password encrypted with refresh token to OS keychain

Step 3: Sync Data Decryption
├─► Download sync payloads from server
├─► Decrypt protected values (`:block/title`, `:block/name`) with AES-256 key
├─► Parse Transit payloads
├─► Apply into local database
└─► Ready to use!
```

**UI Component** ([`frontend.components.e2ee/e2ee-password-to-decrypt-private-key`](../../src/main/frontend/components/e2ee.cljs#L66-L99)):

This modal handles password verification:

- Single password field (no confirmation needed)
- Error feedback if decryption fails
- Enter key submits automatically
- Persists password (encrypted with refresh token) to OS keychain for subsequent sessions

### Data Encryption During Sync

Every time you edit a block in an E2EE graph:

```txt
Local Edit → RTC Operation
            ↓
      Serialize to Transit
            ↓
      Generate Random IV (12 bytes)
            ↓
      Encrypt E2EE Attributes Only
      (`:block/title`, `:block/name`)
        ↓
      Embed encrypted values in tx payload
        ↓
      WebSocket/HTTP → Server
        ↓
      Server Stores mixed payload
      (encrypted protected attrs + plaintext metadata)
            ↓
      Broadcast to Other Clients
            ↓
      Decrypt with AES Key
            ↓
      Apply to Local Database
```

**Security Properties:**

- **IV Uniqueness**: Random 12-byte IV per encryption prevents pattern analysis
- **AEAD**: GCM mode provides both confidentiality and integrity
- **Authentication Tag**: 16 bytes, ensures data hasn't been tampered with

> **Removed:** The previous statement "RTC operation timestamps prevent replay attacks" was removed because replay protection is not implemented as a documented E2EE cryptographic guarantee in current source.

## Security Guarantees

### What E2EE Protects Against

✅ **Server Compromise**

- Even if Logseq servers are hacked, protected fields remain encrypted
- Attacker can access encrypted protected fields, public keys, and plaintext metadata

✅ **Network Eavesdropping**

- HTTPS + E2EE provides double encryption layer
- Even if TLS is broken, data is still encrypted

✅ **Cloud Storage Leaks**

- Protected fields remain unreadable without keys
- Metadata and non-protected attributes may still be visible

✅ **Rogue Employees**

- Logseq staff cannot access your content
- Zero-knowledge architecture by design

### What E2EE Does NOT Protect Against

❌ **Local Device Compromise**

- If your computer is infected with malware, keys in memory can be stolen
- OS keychain can be accessed if device is unlocked

❌ **Weak Passwords**

- PBKDF2 with 600k iterations slows brute-force, but doesn't make weak passwords strong
- Use strong, unique passwords (recommended: 20+ characters, random)

❌ **Phishing Attacks**

- If you enter your password on a fake Logseq site, it can be stolen
- Always verify you're on official Logseq domains

❌ **Physical Access to Unlocked Device**

- If someone has physical access to your unlocked computer, they can access your graphs
- Use full-disk encryption and lock your device when away

❌ **Metadata and Non-Title Properties**

- Graph names, file sizes, and access patterns are not encrypted
- Timestamps of operations may be visible to the server
- Block properties other than `:block/title` and `:block/name` are sent unencrypted
- Assets (images, PDFs) are not encrypted

## Password Best Practices

### Creating a Strong Password

**Recommended Approach**: Use a passphrase with high entropy

✅ **Good Examples**:

- `correct-horse-battery-staple-7821` (Diceware method)
- `T8$mK9@pL2#nQ5&vR3!bW6^xY4*hZ7` (Random generator)
- `MyDog'sName-Was-Fluffy-in-1997!` (Memorable + unique)

❌ **Bad Examples**:

- `password123` (too common, dictionary word)
- `logseq2024` (predictable pattern)
- `qwerty` (keyboard pattern)

**Minimum Recommendation**:

- **20+ characters**
- **Mix of letters, numbers, symbols**
- **Not reused from other services**
- **Not based on personal information**

### Storing Your Password Securely

**Option 1: Password Manager** (Recommended)

- 1Password, Bitwarden, KeePassXC, etc.
- Automatically generates and stores strong passwords
- Protected by master password or biometrics
- Syncs across devices securely

**Option 2: Physical Backup**

- Write password on paper
- Store in safe or locked drawer
- Keep multiple copies in secure locations
- Never store in plain text files

**Option 3: Memory Only** (High Risk)

- Only recommended if you have excellent memory
- High risk of lockout if forgotten
- No recovery mechanism available

### What Happens If You Forget

⚠️ **Critical**: There is **no password recovery mechanism** by design

**Consequences:**

1. All cloud data becomes **permanently inaccessible**
2. You cannot decrypt existing cloud backups
3. Logseq support **cannot help** (they don't have your password)

**Mitigation:**

1. You can still access the **local version** of your graph
2. Local data remains unencrypted and accessible
3. You can create a **new E2EE graph** with a new password

**Recovery Steps:**

```bash
# Access local graph (not encrypted)
1. Open Logseq
2. Switch to local-only mode
3. Your data is still there, unencrypted locally

# Start fresh with cloud sync
1. Create new encrypted graph
2. Set new password (and back it up this time!)
3. Manually migrate content from local graph
```

## Implementation Details

### Cryptography Libraries

Logseq uses the **Web Crypto API** for all cryptographic operations:

```javascript
// Available as global in browsers and Electron
window.crypto.subtle
```

**Advantages:**

- **Native**: Implemented in browser/OS, very fast
- **Audited**: Maintained by browser vendors (Google, Mozilla, Apple, Microsoft)
- **Standards-based**: W3C specification, widely adopted
- **Hardware acceleration**: Can use CPU crypto instructions (AES-NI)

**Electron Support**:

- Uses Chromium's crypto implementation
- Same security guarantees as Chrome browser
- Access to `keytar` library for OS keychain integration (stores encrypted password)

### Key Storage Locations

**Server-side** (encrypted, opaque to server):

- Encrypted private key + public key stored via `/e2ee/user-keys` API
- Per-graph encrypted AES key stored via `/e2ee/graphs/:id/aes-key` API

**Client-side (Desktop)**:

```bash
# OS Keychain stores: E2EE password encrypted with refresh token
# Account: "logseq-encrypted-password"
# Service: "Logseq E2EE"

# macOS: ~/Library/Keychains/login.keychain-db
# Windows: Credential Manager → Windows Credentials
# Linux: Secret Service (GNOME Keyring / KDE Wallet)
```

**Client-side (Mobile)**:

```txt
# Capacitor secure storage
# Key: "logseq-encrypted-password"
# Value: E2EE password encrypted with refresh token
```

**Client-side (Web)**:

```txt
# Worker secret storage (browser platform adapter)
# Backed by IndexedDB KV for secret text
# Key: "logseq-encrypted-password"
# Value: E2EE password encrypted with refresh token
# Also: IndexedDB KV for cached encrypted AES keys
```

**In-memory (all platforms)**:

```clojure
;; Decrypted AES keys cached per graph in worker thread
;; atom: *graph->aes-key {graph-id CryptoKey}
```

### Code References

**Core Encryption Logic**:

- [`frontend.common.crypt`](../../src/main/frontend/common/crypt.cljs) — All cryptographic primitives (~315 lines)
  - `<generate-rsa-key-pair` — Create RSA-4096 key pair
  - `<generate-aes-key` — Create AES-256 key
  - `<encrypt-private-key` — Password-encrypt private key with PBKDF2
  - `<decrypt-private-key` — Decrypt private key with password
  - `<encrypt-uint8array` / `<decrypt-uint8array` — Binary data encryption/decryption
  - `<encrypt-text` / `<decrypt-text` — Transit-encoded text encryption/decryption
  - `<encrypt-aes-key` / `<decrypt-aes-key` — RSA-wrap/unwrap AES keys

**Sync-layer E2EE Logic**:

- [`frontend.worker.sync.crypt`](../../src/main/frontend/worker/sync/crypt.cljs) — E2EE helpers for db-sync (~600 lines)
  - `<ensure-graph-aes-key` — Load or create per-graph AES key
  - `<encrypt-tx-data` / `<decrypt-tx-data` — Encrypt/decrypt sync transactions
  - `<grant-graph-access!` — Share graph AES key with another user via their public key
  - `<change-e2ee-password!` — Re-encrypt private key with new password

**UI Components**:

- [`frontend.components.e2ee`](../../src/main/frontend/components/e2ee.cljs) — E2EE modals (~100 lines)
  - `e2ee-request-new-password` — Initial password setup modal
  - `e2ee-password-to-decrypt-private-key` — Password entry on startup

**Main-thread E2EE Handler**:

- [`frontend.handler.e2ee`](../../src/main/frontend/handler/e2ee.cljs) — Bridges worker thread to keychain/secure-storage (~83 lines)
  - `<keychain-save!` / `<keychain-get` / `<keychain-delete!` — Platform-aware storage (Electron keytar, Capacitor secure storage)

**Keychain Integration (Electron)**:

- [`electron.keychain`](../../src/electron/electron/keychain.cljs) — OS keychain wrapper via `keytar` (~56 lines)
  - `<set-password!` — Store encrypted password in OS keychain
  - `<get-password` — Retrieve encrypted password
  - `<delete-password!` — Remove entry from keychain

**Encrypted Attributes**:

- [`frontend.worker.sync.const/encrypt-attr-set`](../../src/main/frontend/worker/sync/const.cljs) — Defines which attributes are encrypted: `#{:block/title :block/name}`

## Findings: New Capabilities

- Password rotation is implemented via `:thread-api/change-e2ee-password`, which re-encrypts the RSA private key with the new password and persists the updated local secret without re-encrypting all graph content.
- Key reset is available via `:thread-api/reset-user-rsa-key-pair` to generate and upload a new RSA key pair and encrypted private key.
- Graph sharing key handoff is implemented via `/e2ee/graphs/:id/grant-access` using recipient public keys.

## Comparison with Other Systems

### Logseq E2EE vs. Others

| Feature             | Logseq                 | Notion          | Obsidian Sync | Standard Notes |
| ------------------- | ---------------------- | --------------- | ------------- | -------------- |
| Encryption          | ✅ E2EE                | ❌ Server-side  | ✅ E2EE       | ✅ E2EE        |
| Algorithm           | RSA-4096 + AES-256-GCM | AES-256         | AES-256       | AES-256        |
| Zero-knowledge      | ✅ Yes                 | ❌ No           | ✅ Yes        | ✅ Yes         |
| Password derivation | PBKDF2 (600k)          | N/A             | PBKDF2        | PBKDF2 (110k)  |
| Key storage         | OS Keychain            | Server          | OS Keychain   | Secure Enclave |
| Recovery option     | ❌ No                  | ✅ Admin access | ❌ No         | ✅ Key export  |
| Metadata encrypted  | ❌ No                  | ❌ No           | ✅ Yes        | ✅ Yes         |

**Logseq's Strengths:**

- Strong RSA key size (4096 vs. 2048 typical)
- Very high PBKDF2 iterations (600k vs. 100k-110k typical)
- Hybrid cryptography (RSA + AES) for key sharing

**Logseq's Weaknesses:**

- No password recovery mechanism
- Only block titles and page names are encrypted; other properties, assets, and metadata are not yet encrypted
- No key export/backup feature yet

## Frequently Asked Questions

### General Questions

**Q: Is E2EE enabled by default?**

A: No, E2EE is **opt-in** for remote graphs. Local-only graphs are not encrypted (stored as plain markdown/SQLite).

**Q: Can I use E2EE with local-only graphs?**

A: No, E2EE only applies to remote/cloud-synced graphs. Local graphs are stored unencrypted on your file system.

**Q: Does E2EE slow down Logseq?**

A: Minimal impact. Encryption/decryption happens in background threads. You might notice:

- Initial setup: 2-5 seconds (RSA key generation)
- Normal usage: No perceivable slowdown
- Large imports: Slightly slower than unencrypted

**Q: Can I share an E2EE graph with others?**

A: Yes! When you share with collaborators:

1. Their public key is used to encrypt the graph's AES key
2. They decrypt using their own password
3. Each user has their own password, but shares the same AES key for the graph

### Technical Questions

**Q: What happens if I change my password?**

A: Changing your E2EE password re-encrypts your RSA private key and updates persisted local secret storage. It does not re-encrypt graph AES data.

1. Decrypt private key with old password
2. Re-encrypt private key with new password
3. Upload updated encrypted private key
4. Persist new password secret locally

**Q: Are assets (images, PDFs) encrypted?**

A: No. Currently, only block titles (`:block/title`) and page names (`:block/name`) are encrypted during sync. Assets and other properties are not encrypted. This may change in future releases.

**Q: How does E2EE work with Real-Time Collaboration?**

A: RTC payloads encrypt protected attributes before sync transport:

1. Local edit → Generate RTC operation
2. Encrypt protected values (`:block/title`, `:block/name`) with AES-256
3. Send operation payload to server
4. Server broadcasts payload
5. Other clients decrypt protected values with shared AES key

See [Real-Time Sync Tutorial](../../tutorials/collaboration/real-time-sync.md) for details.

**Q: Can the Logseq team see my password during login?**

A: No. Authentication and encryption are separate:

- **Authentication**: OAuth/JWT for server access (username/password for Logseq account)
- **E2EE Password**: Never sent to server, only used locally for encryption

**Q: What if the Web Crypto API has vulnerabilities?**

A: Web Crypto API is maintained by browser vendors with dedicated security teams. If vulnerabilities are found:

- Browser updates fix them automatically
- Logseq releases new Electron version with updated Chromium
- Standard cryptographic review processes apply

**Q: Does E2EE protect against supply chain attacks?**

A: Partially. If Logseq's code is compromised:

- ✅ Existing encrypted data is safe (already encrypted)
- ❌ Malicious code could steal keys from memory
- ⚠️ Always update from official sources only
- ⚠️ Consider using version pinning for critical data

### Troubleshooting

**Q: "Wrong password" error but I know it's correct**

Possible causes:

1. **Caps Lock is on** (passwords are case-sensitive)
2. **Keyboard layout changed** (especially for special characters)
3. **Copy-paste added whitespace** (spaces before/after password)
4. **Different device** (keychain not synced)

**Q: Lost access to OS keychain**

Solutions:

1. Restore keychain from backup (Time Machine on macOS, etc.)
2. If no backup: Create new E2EE graph with new password
3. Manually migrate content from local graph

**Q: E2EE setup keeps failing**

Check:

1. Stable internet connection (needs to reach Logseq servers)
2. Browser/Electron version updated
3. Sufficient memory (RSA-4096 generation is memory-intensive)
4. No browser extensions interfering with Web Crypto API

## Future Improvements

### Planned Features

🟡 **In Progress**:

- Export/import encrypted backups
- Multi-device key synchronization

> **Removed:** "Password change without full re-encryption" moved out of in-progress because password rotation support is already implemented in current worker APIs.

🔜 **Planned**:

- Encrypt all block properties (currently only `:block/title` and `:block/name`)
- Asset encryption (images, PDFs, files)
- Hardware security key support (YubiKey, etc.)
- Biometric unlock (Touch ID, Face ID)
- Metadata encryption (graph names, timestamps)
- Key rotation and versioning
- Recovery codes (with trade-offs clearly communicated)

### Research Areas

- Post-quantum cryptography (preparing for quantum computers)
- Zero-knowledge proofs for collaboration
- Homomorphic encryption (query encrypted data without decrypting)
- Threshold cryptography (split keys across devices)

## Conclusion

Logseq's E2EE implementation provides strong privacy protection for encrypted sync attributes using industry-standard cryptography:

- **RSA-4096 + AES-256-GCM**: State-of-the-art algorithms
- **PBKDF2 with 600k iterations**: Resists brute-force attacks
- **OS keychain integration**: Secure key storage
- **Zero-knowledge architecture**: Server never sees your data

**Remember**: E2EE is only as strong as your password. Use a strong, unique password and store it securely in a password manager or physical backup.

For collaboration features using E2EE, see the [Real-Time Sync Tutorial](../../tutorials/collaboration/real-time-sync.md).

For user profile configuration, see [User Profiles Reference](../../reference/features/user-profiles.md).

---

**Last Updated**: May 2026
**Status**: 🟡 Active Development (E2EE feature evolving with RTC improvements)  
**Feedback**: Report E2EE issues on [GitHub](https://github.com/logseq/logseq/issues) with `encryption` label
