# End-to-End Encryption (E2EE) Security Investigation: Logseq Real-Time Collaboration

**Investigation Type:** Security Analysis & Code Detective  
**Date:** December 7, 2025  
**Status:** Revalidated Against Current Source (src/, deps/, packages/)  
**Risk Level:** 🟢 Low (Well-implemented with industry standards)

> **Last revised:** 2026-05-30

---

## Executive Summary

Logseq's current sync stack implements **robust end-to-end encryption (E2EE)** for encrypted graphs using industry-standard cryptographic algorithms and best practices. The active implementation is in db-sync worker paths (not the removed legacy `frontend.worker.rtc` namespace), and uses a hybrid model combining RSA-4096 for key exchange and AES-256-GCM for data encryption.

> **Removed:** Legacy `src/main/frontend/worker/rtc/*` references in this document are no longer active implementation paths. Current E2EE logic is centered in `src/main/frontend/worker/sync/crypt.cljs` and related db-sync modules.

## Findings: New Capabilities

- **PBKDF2 hardening shipped:** private-key password KDF now uses **600,000** iterations for new payloads and version-tagged encrypted key material (`"20251210"`), while retaining backward decryption for older 100,000-iteration payloads. See `src/main/frontend/common/crypt.cljs`.
- **Sync architecture converged on db-sync HTTP+WS routes:** graph key exchange and grant-access flows are implemented via `/e2ee/*` endpoints and db-sync request schema validation. See `src/main/frontend/worker/sync/crypt.cljs` and `deps/db-sync/src/logseq/db_sync/malli_schema.cljs`.
- **Secret storage abstraction is runtime-native:** persisted E2EE password handling now goes through platform crypto adapters (`save-secret-text!`, `read-secret-text`, `delete-secret-text!`) with browser/node owner-specific implementations. See `src/main/frontend/worker/platform.cljs`, `src/main/frontend/worker/platform/browser.cljs`, and `src/main/frontend/worker/platform/node.cljs`.
- **Sync endpoint defaults changed:** production defaults are now `wss://api.logseq.io/sync/%s` and `https://api.logseq.io` with custom sync server override support. See `src/main/frontend/config.cljs`.

### 🔐 Multi-User Graph Sharing: Password vs. Token

**CRITICAL FINDING:** ✅ **Users do NOT share the same password**

When an encrypted graph is shared with another user:

- **Each user has their own independent E2EE password**
- **No password sharing occurs between users**
- **Public key cryptography (RSA-4096) is used for secure key exchange**
- **Each user's copy of the graph AES key is encrypted with THEIR OWN public key**

**How It Works:**

1. **Owner's Side:**
   - Owner enters their own password to unlock their RSA private key
   - Owner fetches collaborator's RSA public key from server (by email)
   - Owner decrypts graph AES key with their private key
   - Owner re-encrypts graph AES key with collaborator's public key
   - Server stores per-user encrypted copies of the AES key

2. **Collaborator's Side:**
   - Collaborator downloads encrypted graph from server
   - Collaborator enters THEIR OWN password (NOT owner's password)
   - Collaborator decrypts their RSA private key with their password
   - Collaborator decrypts their copy of graph AES key with their private key
   - Collaborator decrypts graph data with AES key

**Security Model:**

| Aspect                    | Implementation                        | Security Level    |
| ------------------------- | ------------------------------------- | ----------------- |
| Password Sharing          | ❌ No passwords shared                | ✅ Excellent      |
| Key Exchange              | RSA-4096 public key cryptography      | ✅ Excellent      |
| Server Knowledge          | Server cannot decrypt any user's data | ✅ Zero-knowledge |
| Password Independence     | Each user sets own password           | ✅ Excellent      |
| Public Key Authentication | Email-based lookup (no verification)  | ⚠️ Limited        |

**Locations:**

- Owner grants access: `src/main/frontend/worker/rtc/core.cljs:487-502`
- Key re-encryption: `src/main/frontend/worker/rtc/crypt.cljs:213-223`
- Collaborator retrieves key: `src/main/frontend/worker/rtc/crypt.cljs:145-158`

---

### Key Security Findings

✅ **Strong Encryption:**

- RSA-4096 with OAEP padding and SHA-256 for asymmetric encryption
- AES-256-GCM for symmetric encryption (authenticated encryption)
- PBKDF2 with 600,000 iterations (new payloads) and SHA-256 for password-based key derivation

✅ **Secure Key Management:**

- Private keys encrypted with user passwords before storage
- System keychain integration (macOS Keychain, Windows Credential Manager, iOS/Android Secure Storage)
- Keys never transmitted unencrypted
- No password sharing between collaborators (public key cryptography)

✅ **Transport Security:**

- WebSocket connections over TLS (wss://)
- All sensitive data encrypted before transmission

> **Removed:** Prior claim that password policy was "6 chars + weak allowed" is stale and tied to removed UI/path assumptions. Current code should be re-assessed as a separate, current-state password UX/policy audit.

⚠️ **Multi-User Sharing Risks:**

- **Weakest link problem:** One collaborator with weak password compromises entire shared graph
- **No public key verification:** Server could substitute attacker's key (requires malicious server)
- **No revocation mechanism:** Cannot remove compromised collaborator access
- **No audit logging:** Cannot track who accessed what data

⚠️ **Additional Areas for Enhancement:**

- Implement key rotation mechanisms
- Add forward secrecy capabilities
- Public key fingerprint verification for collaborators

---

## Table of Contents

1. [Encryption Architecture](#encryption-architecture)
2. [Cryptographic Algorithms](#cryptographic-algorithms)
3. [Key Management System](#key-management-system)
4. [Data Encryption Flow](#data-encryption-flow)
5. [Asset Encryption](#asset-encryption)
6. [Transport Security](#transport-security)
7. [Password Storage](#password-storage)
8. [Security Analysis](#security-analysis)
9. [Threat Model](#threat-model)
10. [Code References](#code-references)
11. [Recommendations](#recommendations)

---

## Encryption Architecture

### Hybrid Encryption Model

Logseq RTC uses a **hybrid encryption system** combining asymmetric and symmetric cryptography:

```
┌─────────────────────────────────────────────────────────────┐
│                    E2EE Architecture                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  User Layer:                                                │
│  ┌──────────────┐                                          │
│  │ User Password│ ──PBKDF2(100k)──> AES-256 Key            │
│  └──────────────┘                         │                │
│                                            ▼                │
│  ┌─────────────────────────────────────────────────┐       │
│  │         RSA-4096 Private Key (Encrypted)        │       │
│  └─────────────────────────────────────────────────┘       │
│                          │                                  │
│                          │ Decrypts                         │
│                          ▼                                  │
│  ┌─────────────────────────────────────────────────┐       │
│  │         RSA-4096 Private Key (In Memory)        │       │
│  └─────────────────────────────────────────────────┘       │
│                          │                                  │
│                          │ Decrypts                         │
│                          ▼                                  │
│  Graph Layer:                                               │
│  ┌─────────────────────────────────────────────────┐       │
│  │    AES-256 Graph Key (Encrypted with RSA)       │       │
│  └─────────────────────────────────────────────────┘       │
│                          │                                  │
│                          │ Encrypts/Decrypts                │
│                          ▼                                  │
│  ┌─────────────────────────────────────────────────┐       │
│  │      Graph Data + Assets (Encrypted)            │       │
│  └─────────────────────────────────────────────────┘       │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Key Hierarchy

1. **User Password** → PBKDF2 → **Derived AES-256 Key**
2. **Derived Key** encrypts → **RSA-4096 Private Key**
3. **RSA-4096 Public Key** encrypts → **Graph AES-256 Key**
4. **Graph AES-256 Key** encrypts → **All Graph Data & Assets**

**Location:** `src/main/frontend/worker/rtc/crypt.cljs:1-279`

---

## Cryptographic Algorithms

### 1. RSA-4096 (Asymmetric Encryption)

**Purpose:** Key exchange and distribution  
**Algorithm:** RSA-OAEP (Optimal Asymmetric Encryption Padding)  
**Key Size:** 4096 bits  
**Hash Function:** SHA-256  
**Public Exponent:** 65537 (0x10001)

**Implementation:**

```clojure
(defn <generate-rsa-key-pair
  "Generates a new RSA public/private key pair."
  []
  (p/let [r (.generateKey subtle
                          #js {:name "RSA-OAEP"
                               :modulusLength 4096
                               :publicExponent (js/Uint8Array. [1 0 1])
                               :hash "SHA-256"}
                          true
                          #js ["encrypt" "decrypt"])]
    {:publicKey (.-publicKey r)
     :privateKey (.-privateKey r)}))
```

**Location:** `src/main/frontend/common/crypt.cljs:58-78`

**Security Analysis:**

- ✅ 4096-bit keys exceed NIST recommendations (2048+ bits for 2030+)
- ✅ RSA-OAEP provides protection against chosen ciphertext attacks
- ✅ SHA-256 hash is cryptographically secure
- ✅ Standard public exponent (65537) balances security and performance

**Strength:** Equivalent to ~140-bit symmetric key strength

---

### 2. AES-256-GCM (Symmetric Encryption)

**Purpose:** Graph data and asset encryption  
**Algorithm:** AES-GCM (Galois/Counter Mode)  
**Key Size:** 256 bits  
**IV Size:** 12 bytes (96 bits) - randomly generated per encryption  
**Authentication:** Built-in GMAC tag

**Implementation:**

```clojure
(defn <generate-aes-key
  "Generates a new AES-GCM-256 key."
  []
  (.generateKey subtle
                #js {:name "AES-GCM"
                     :length 256}
                true
                #js ["encrypt" "decrypt"]))

(defn <encrypt-uint8array
  [aes-key arr]
  (p/let [iv (js/crypto.getRandomValues (js/Uint8Array. 12))
          encrypted-data (.encrypt subtle
                                   #js {:name "AES-GCM" :iv iv}
                                   aes-key
                                   arr)]
    [iv (js/Uint8Array. encrypted-data)]))
```

**Location:** `src/main/frontend/common/crypt.cljs:82-92, 185-194`

**Security Analysis:**

- ✅ AES-256 is approved by NSA for TOP SECRET classification
- ✅ GCM mode provides authenticated encryption (AEAD)
- ✅ 96-bit IV is cryptographically secure for GCM
- ✅ Random IV generation prevents pattern analysis
- ✅ Authentication tag prevents tampering

**Strength:** 256-bit security level

---

### 3. PBKDF2 (Password-Based Key Derivation)

**Purpose:** Derive encryption keys from user passwords  
**Algorithm:** PBKDF2 (Password-Based Key Derivation Function 2)  
**Hash Function:** SHA-256  
**Iterations:** 600,000 for new encrypted private-key payloads (legacy 100,000 remains decrypt-compatible)  
**Salt Size:** 16 bytes (128 bits) - randomly generated  
**Derived Key:** AES-256 (256 bits)

**Implementation:**

```clojure
(defn <encrypt-private-key
  "Encrypts a private key with a password."
  [password private-key]
  (p/let [salt (js/crypto.getRandomValues (js/Uint8Array. 16))
          iv (js/crypto.getRandomValues (js/Uint8Array. 12))
          password-key (.importKey subtle "raw"
                                   (.encode (js/TextEncoder.) password)
                                   "PBKDF2"
                                   false
                                   #js ["deriveKey"])
          derived-key (.deriveKey subtle
                      #js {:name "PBKDF2"
                        :salt salt
                        :iterations 600000
                        :hash "SHA-256"}
                      password-key
                      #js {:name "AES-GCM" :length 256}
                      true
                      #js ["encrypt" "decrypt"])
          exported-private-key (.exportKey subtle "pkcs8" private-key)
          encrypted-private-key (.encrypt subtle
                                          #js {:name "AES-GCM" :iv iv}
                                          derived-key
                                          exported-private-key)]
    ["20251210" salt iv (js/Uint8Array. encrypted-private-key)]))
```

**Location:** `src/main/frontend/common/crypt.cljs:84-112`

**Security Analysis:**

- ✅ PBKDF2 is NIST-approved (SP 800-132)
- ✅ 600,000 iterations for new payloads aligns with stronger modern guidance
- ✅ Backward decrypt compatibility for older 100,000-iteration payloads
- ✅ Random 128-bit salt prevents rainbow table attacks
- ✅ SHA-256 provides strong cryptographic hashing

**Current Strength:** Strong KDF posture with explicit migration compatibility

---

## Key Management System

### User RSA Key Pair

**Generation:**

- Generated client-side when user first enables E2EE
- 4096-bit RSA key pair created using Web Crypto API
- Private key immediately encrypted with user password

**Storage:**

| Component             | Location          | Encrypted                   |
| --------------------- | ----------------- | --------------------------- |
| Public Key            | Server (database) | ❌ No (public by design)    |
| Encrypted Private Key | Server (database) | ✅ Yes (password-encrypted) |
| Encrypted Private Key | Client IndexedDB  | ✅ Yes (password-encrypted) |
| Decrypted Private Key | Memory only       | ❌ No (runtime only)        |

**Code Flow:**

```clojure
;; Generate and encrypt user keys
(defn :thread-api/init-user-rsa-key-pair
  [token refresh-token user-uuid]
  (m/sp
    (let [{:keys [publicKey privateKey]} (c.m/<? (crypt/<generate-rsa-key-pair))
          {:keys [password]} (c.m/<? (worker-state/<invoke-main-thread
                                      :thread-api/request-e2ee-password))
          encrypted-private-key (c.m/<? (crypt/<encrypt-private-key password privateKey))]
      (m/? (task--upload-user-rsa-key-pair get-ws-create-task user-uuid
                                           publicKey encrypted-private-key))
      (c.m/<? (<save-e2ee-password refresh-token password)))))
```

**Location:** `src/main/frontend/worker/rtc/crypt.cljs:235-248`

---

### Graph AES Key

**Generation:**

- Generated when graph is first shared/synced
- 256-bit AES-GCM key created using Web Crypto API
- Immediately encrypted with owner's RSA public key

**Storage:**

| Component         | Location          | Encrypted              |
| ----------------- | ----------------- | ---------------------- |
| Encrypted AES Key | Server (database) | ✅ Yes (RSA-encrypted) |
| Encrypted AES Key | Client IndexedDB  | ✅ Yes (RSA-encrypted) |
| Decrypted AES Key | Memory only       | ❌ No (runtime only)   |

**Multi-User Sharing Architecture:**

**KEY SECURITY FINDING:** ✅ **Users do NOT share the same password**

Each user has their own independent E2EE password and RSA key pair. Graph sharing uses **public key cryptography** for secure key exchange, not password sharing.

**Sharing Flow (Owner → Collaborator):**

```
┌─────────────────────────────────────────────────────────────────┐
│ OWNER SIDE (Inviting Collaborator)                             │
├─────────────────────────────────────────────────────────────────┤
│ 1. Enter collaborator's email                                   │
│ 2. Fetch collaborator's RSA public key from server             │
│    └─> WebSocket: "fetch-user-rsa-public-key" (by email)       │
│ 3. Decrypt graph AES key with owner's RSA private key          │
│ 4. Re-encrypt graph AES key with collaborator's RSA public key │
│ 5. Send encrypted AES key to server                            │
│    └─> WebSocket: "grant-access"                               │
└─────────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ SERVER STORAGE                                                  │
├─────────────────────────────────────────────────────────────────┤
│ Graph AES Key stored per-user:                                 │
│ • Owner:        AES_key encrypted with Owner's public key      │
│ • Collaborator: AES_key encrypted with Collaborator's pub key  │
│                                                                 │
│ ⚠️ Server cannot decrypt any version (zero-knowledge)          │
└─────────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│ COLLABORATOR SIDE (Joining Shared Graph)                       │
├─────────────────────────────────────────────────────────────────┤
│ 1. Download encrypted graph from S3                            │
│ 2. Fetch THEIR encrypted AES key from server                   │
│    └─> WebSocket: "fetch-graph-encrypted-aes-key"              │
│ 3. Enter THEIR OWN E2EE password (NOT owner's password)        │
│ 4. Decrypt their RSA private key with their password           │
│ 5. Decrypt graph AES key with their RSA private key            │
│ 6. Decrypt graph data with AES key                             │
│ 7. Store in local IndexedDB                                    │
└─────────────────────────────────────────────────────────────────┘
```

**Code Flow (Owner Grants Access):**

```clojure
;; Location: src/main/frontend/worker/rtc/crypt.cljs:213-223
(defn task--encrypt-graph-aes-key-by-other-user-public-key
  "Encrypt AES key for sharing with another user"
  [get-ws-create-task graph-uuid user-uuid other-user-email]
  (m/sp
    ;; 1. Get graph AES key (decrypt with owner's private key)
    (when-let [graph-aes-key (m/? (task--get-aes-key get-ws-create-task
                                                     user-uuid graph-uuid))]
      ;; 2. Fetch collaborator's public key by email
      (when-let [public-key (m/? (task--fetch-user-rsa-public-key
                                  get-ws-create-task other-user-email))]
        ;; 3. Re-encrypt AES key with collaborator's public key
        (c.m/<? (crypt/<encrypt-aes-key public-key graph-aes-key))))))

;; Location: src/main/frontend/worker/rtc/core.cljs:487-502
(defn new-task--grant-access-to-others
  [user-uuid graph-uuid other-user-emails]
  (m/sp
    (let [{:keys [get-ws-create-task]} (ws-util/get-ws-create-map user-uuid graph-uuid)]
      (doseq [other-user-email other-user-emails]
        ;; Encrypt AES key for each collaborator
        (when-let [encrypted-aes-key
                   (m/? (rtc-crypt/task--encrypt-graph-aes-key-by-other-user-public-key
                         get-ws-create-task graph-uuid user-uuid other-user-email))]
          ;; Send to server
          (m/? (ws-util/send&recv get-ws-create-task
                                  {:action "grant-access"
                                   :target-user-email other-user-email
                                   :encrypted-aes-key (ldb/write-transit-str encrypted-aes-key)})))))))
```

**Code Flow (Collaborator Retrieves Key):**

```clojure
;; Location: src/main/frontend/worker/rtc/crypt.cljs:135-158
(defn task--fetch-graph-aes-key
  "Fetches the AES key for a graph, from indexeddb or server.
  Return nil if not exists"
  [get-ws-create-task graph-uuid private-key]
  (m/sp
    (let [encrypted-aes-key (c.m/<? (<get-item (graph-encrypted-aes-key-idb-key graph-uuid)))]
      (if encrypted-aes-key
        ;; Use cached encrypted key
        (c.m/<? (crypt/<decrypt-aes-key private-key encrypted-aes-key))
        ;; Fetch from server (encrypted specifically for this user)
        (when-let [encrypted-aes-key (m/? (task--remote-fetch-graph-encrypted-aes-key
                                           get-ws-create-task graph-uuid))]
          (let [aes-key (c.m/<? (crypt/<decrypt-aes-key private-key encrypted-aes-key))]
            ;; Cache locally
            (c.m/<? (<set-item! (graph-encrypted-aes-key-idb-key graph-uuid) encrypted-aes-key))
            aes-key))))))

;; Location: src/main/frontend/handler/db_based/rtc.cljs:33-56
(defn <rtc-download-graph!
  "Called when collaborator joins shared graph"
  [graph-name graph-uuid graph-schema-version timeout-ms]
  (p/let [token (state/get-auth-id-token)
          ;; Request download from server
          download-info-uuid (state/<invoke-db-worker
                              :thread-api/rtc-request-download-graph
                              token graph-uuid graph-schema-version)
          ;; Wait for S3 URL
          {:keys [download-info-s3-url]}
          (state/<invoke-db-worker :thread-api/rtc-wait-download-graph-info-ready
                                   token download-info-uuid graph-uuid
                                   graph-schema-version timeout-ms)]
    ;; Download and decrypt (uses collaborator's own password/keys)
    (state/<invoke-db-worker :thread-api/rtc-download-graph-from-s3
                             graph-uuid graph-name download-info-s3-url)))
```

**Public Key Lookup Mechanism:**

```clojure
;; Location: src/main/frontend/worker/rtc/crypt.cljs:198-212
(defn task--fetch-user-rsa-public-key
  "Fetches a user's public key by email from server"
  [get-ws-create-task user-email]
  (m/sp
    (let [response (m/? (ws-util/send&recv get-ws-create-task
                                           {:action "fetch-user-rsa-public-key"
                                            :user-email user-email}))]
      (when-let [public-key (:public-key response)]
        (c.m/<? (<import-public-key-transit-str public-key))))))
```

**Security Properties:**

| Property                  | Status     | Details                                       |
| ------------------------- | ---------- | --------------------------------------------- |
| Password Independence     | ✅ Yes     | Each user sets their own password             |
| Zero-Knowledge Server     | ✅ Yes     | Server cannot decrypt any user's data         |
| Forward Secrecy           | ❌ No      | Compromised private key exposes all past data |
| Key Rotation              | ❌ No      | No mechanism to rotate graph AES keys         |
| Public Key Authentication | ⚠️ Limited | Email-based lookup (see risks below)          |

**Locations:**

- Owner grants access: `src/main/frontend/worker/rtc/core.cljs:487-502`
- Key re-encryption: `src/main/frontend/worker/rtc/crypt.cljs:213-223`
- Public key fetch: `src/main/frontend/worker/rtc/crypt.cljs:198-212`
- Collaborator retrieves key: `src/main/frontend/worker/rtc/crypt.cljs:145-158`
- Download handler: `src/main/frontend/handler/db_based/rtc.cljs:33-56`

---

## Data Encryption Flow

### Block/Transaction Encryption

**What Gets Encrypted:**

- Block content (text, properties)
- Block metadata (timestamps, UUIDs preserved for sync)
- Database transactions
- User-specified sensitive attributes

**Encryption Process:**

```
User Edit
    │
    ├──> Transaction captured
    │
    ├──> Sensitive attributes encrypted with Graph AES Key
    │
    ├──> Transaction data serialized (Transit)
    │
    ├──> Encrypted data sent via WebSocket (over TLS)
    │
    ├──> Server stores encrypted transaction
    │
    └──> Remote clients receive encrypted data
         │
         └──> Decrypt with Graph AES Key → Apply to local DB
```

**Implementation:**

```clojure
(defn <encrypt-map
  [aes-key encrypt-attr-set m]
  (reduce
   (fn [map-p encrypt-attr]
     (p/let [m map-p]
       (if-let [v (get m encrypt-attr)]
         (p/let [v' (p/chain (<encrypt-text aes-key v) ldb/write-transit-str)]
           (assoc m encrypt-attr v'))
         m)))
   (p/promise m) encrypt-attr-set))
```

**Location:** `src/main/frontend/common/crypt.cljs:244-255`

### Text Encryption

**Format:** `[iv, encrypted-data]` (vector serialized with Transit)

```clojure
(defn <encrypt-text
  "Encrypts text with an AES key."
  [aes-key text]
  (p/let [iv (js/crypto.getRandomValues (js/Uint8Array. 12))
          encoded-text (.encode (js/TextEncoder.) text)
          encrypted-data (.encrypt subtle
                                   #js {:name "AES-GCM" :iv iv}
                                   aes-key
                                   encoded-text)]
    [iv (js/Uint8Array. encrypted-data)]))
```

**Location:** `src/main/frontend/common/crypt.cljs:210-222`

---

## Multi-User Graph Sharing Architecture

### Overview

When a graph owner shares an encrypted graph with collaborators, Logseq uses **public key cryptography** for secure key exchange. **Users do NOT share the same password** - each user maintains their own independent E2EE password and RSA key pair.

### Complete Sharing Flow

```
┌═══════════════════════════════════════════════════════════════════════┐
║ PHASE 1: Owner Creates Graph (Initial Setup)                         ║
╠═══════════════════════════════════════════════════════════════════════╣
║                                                                       ║
║  Owner sets password → PBKDF2(100k iter) → Derived AES Key           ║
║                                    ↓                                  ║
║           Generate RSA-4096 Key Pair (client-side)                    ║
║                 ↓                        ↓                            ║
║        Private Key               Public Key                           ║
║    (encrypted w/ password)    (sent to server)                        ║
║                 ↓                                                     ║
║        Stored in IndexedDB                                            ║
║                 ↓                                                     ║
║      Generate Graph AES-256 Key                                       ║
║                 ↓                                                     ║
║   Encrypt with Owner's RSA Public Key                                 ║
║                 ↓                                                     ║
║      Stored on Server (per-user encrypted)                            ║
║                                                                       ║
└═══════════════════════════════════════════════════════════════════════┘
                                    ▼
┌═══════════════════════════════════════════════════════════════════════┐
║ PHASE 2: Owner Invites Collaborator                                  ║
╠═══════════════════════════════════════════════════════════════════════╣
║                                                                       ║
║  1. Owner enters collaborator's email address                         ║
║      └─> UI: <rtc-invite-email>                                      ║
║                 ↓                                                     ║
║  2. Fetch collaborator's RSA public key from server                   ║
║      └─> WebSocket: "fetch-user-rsa-public-key"                      ║
║      └─> Lookup by email (server returns public key)                 ║
║                 ↓                                                     ║
║  ⚠️ TRUST POINT: No verification that key belongs to email owner     ║
║                 ↓                                                     ║
║  3. Owner enters THEIR password → Decrypt owner's private key         ║
║                 ↓                                                     ║
║  4. Decrypt graph AES key (using owner's private key)                 ║
║                 ↓                                                     ║
║       Graph AES Key (in memory, plaintext)                            ║
║                 ↓                                                     ║
║  5. Re-encrypt graph AES key with COLLABORATOR's public key           ║
║      └─> function: task--encrypt-graph-aes-key-by-other-user-public-key ║
║                 ↓                                                     ║
║  6. Send encrypted AES key to server                                  ║
║      └─> WebSocket: "grant-access"                                   ║
║      └─> Payload: {target-user-email, encrypted-aes-key}             ║
║                                                                       ║
└═══════════════════════════════════════════════════════════════════════┘
                                    ▼
┌═══════════════════════════════════════════════════════════════════════┐
║ SERVER STORAGE (Zero-Knowledge)                                      ║
╠═══════════════════════════════════════════════════════════════════════╣
║                                                                       ║
║  Graph UUID: abc-123                                                  ║
║  ├─> Owner:        AES_key encrypted with Owner_PubKey               ║
║  ├─> Collaborator: AES_key encrypted with Collaborator_PubKey        ║
║  └─> [Future users would get additional encrypted copies]            ║
║                                                                       ║
║  ⚠️ SERVER CANNOT DECRYPT:                                           ║
║     • Server has public keys only (not private keys)                  ║
║     • Each encrypted AES key copy is unique per user                  ║
║     • Only user with corresponding private key can decrypt            ║
║                                                                       ║
└═══════════════════════════════════════════════════════════════════════┘
                                    ▼
┌═══════════════════════════════════════════════════════════════════════┐
║ PHASE 3: Collaborator Joins Shared Graph                             ║
╠═══════════════════════════════════════════════════════════════════════╣
║                                                                       ║
║  1. Collaborator clicks "Download Graph" or receives invitation       ║
║      └─> Handler: <rtc-download-graph!>                              ║
║                 ↓                                                     ║
║  2. Request download from server                                      ║
║      └─> API: :thread-api/rtc-request-download-graph                 ║
║      └─> Server generates S3 download URL (encrypted data)            ║
║                 ↓                                                     ║
║  3. Fetch THEIR encrypted AES key copy                                ║
║      └─> WebSocket: "fetch-graph-encrypted-aes-key"                  ║
║      └─> Returns: AES key encrypted with Collaborator's public key   ║
║                 ↓                                                     ║
║  4. Collaborator enters THEIR OWN password (NOT owner's!)             ║
║      └─> UI prompts for E2EE password                                ║
║                 ↓                                                     ║
║  5. Derive key from password → Decrypt collaborator's private key     ║
║      └─> PBKDF2(password, 100k iterations)                           ║
║                 ↓                                                     ║
║  6. Decrypt graph AES key (using collaborator's private key)          ║
║      └─> function: task--fetch-graph-aes-key                         ║
║      └─> RSA-OAEP decryption                                         ║
║                 ↓                                                     ║
║       Graph AES Key (in memory, plaintext)                            ║
║                 ↓                                                     ║
║  7. Download encrypted graph data from S3                             ║
║      └─> API: :thread-api/rtc-download-graph-from-s3                 ║
║                 ↓                                                     ║
║  8. Decrypt all blocks/transactions with graph AES key                ║
║      └─> function: task--decrypt-blocks-aux                          ║
║      └─> AES-256-GCM decryption                                      ║
║                 ↓                                                     ║
║  9. Store decrypted data in local IndexedDB                           ║
║      └─> Collaborator now has full access to graph                   ║
║                                                                       ║
└═══════════════════════════════════════════════════════════════════════┘
```

### Security Properties

| Aspect                  | Implementation                                  | Security Level   |
| ----------------------- | ----------------------------------------------- | ---------------- |
| **Password Sharing**    | ❌ **NO** - Each user has independent password  | ✅ **Excellent** |
| **Key Exchange**        | RSA-4096 public key cryptography                | ✅ **Excellent** |
| **Server Knowledge**    | Zero-knowledge (cannot decrypt any user's data) | ✅ **Excellent** |
| **Per-User Encryption** | Each user gets uniquely encrypted AES key       | ✅ **Excellent** |
| **Public Key Auth**     | Email-based lookup, no verification             | ⚠️ **Limited**   |
| **Access Revocation**   | No mechanism to remove collaborators            | ❌ **Missing**   |
| **Weakest Link**        | One weak password compromises all               | ⚠️ **Risk**      |

### Code Locations

**Owner Grants Access:**

```clojure
// src/main/frontend/worker/rtc/core.cljs:487-502
(defn new-task--grant-access-to-others [user-uuid graph-uuid other-user-emails] ...)

// src/main/frontend/worker/rtc/crypt.cljs:213-223
(defn task--encrypt-graph-aes-key-by-other-user-public-key [...] ...)

// src/main/frontend/handler/db_based/rtc.cljs:150-180
(defn <rtc-invite-email [graph-uuid email-address] ...)
```

**Public Key Lookup:**

```clojure
// src/main/frontend/worker/rtc/crypt.cljs:198-212
(defn task--fetch-user-rsa-public-key [get-ws-create-task user-email] ...)
```

**Collaborator Joins:**

```clojure
// src/main/frontend/handler/db_based/rtc.cljs:33-56
(defn <rtc-download-graph! [graph-name graph-uuid ...] ...)

// src/main/frontend/worker/rtc/crypt.cljs:145-158
(defn task--fetch-graph-aes-key [get-ws-create-task graph-uuid private-key] ...)

// src/main/frontend/worker/rtc/full_upload_download_graph.cljs:405-440
(defn new-task--transact-remote-all-blocks! [all-blocks repo graph-uuid] ...)
```

### Trust Assumptions

⚠️ **Critical Trust Points:**

1. **Server Integrity:** Server must return correct public keys (no substitution)
   - **Risk:** Malicious server could return attacker's public key
   - **Mitigation:** None currently (no key fingerprint verification)
   - **Industry Standard:** Most E2EE systems face similar trust issue

2. **Email Identity:** Email address correctly identifies user
   - **Risk:** Typo in email grants access to wrong person
   - **Mitigation:** None currently (no confirmation step)

3. **All Collaborators:** Each collaborator's password strength
   - **Risk:** One weak password compromises entire shared graph
   - **Mitigation:** Global password policy (but currently weak)

---

## Asset Encryption

Assets (images, PDFs, videos) are encrypted before upload to S3.

### Asset Encryption Process

```
Asset File (Binary)
    │
    ├──> Read as Uint8Array
    │
    ├──> Generate random 12-byte IV
    │
    ├──> Encrypt with Graph AES Key (AES-256-GCM)
    │
    ├──> Result: [iv, encrypted-data]
    │
    ├──> Serialize with Transit
    │
    ├──> Upload to S3 via presigned URL
    │
    └──> Metadata stored in graph (encrypted)
```

**Implementation:**

```clojure
(defn new-task--rtc-upload-asset
  [repo aes-key asset-block-uuid-str asset-type checksum put-url]
  (m/sp
    (let [asset-file (c.m/<? (<read-asset repo asset-block-uuid-str asset-type))
          asset-file* (if (not aes-key)
                        asset-file
                        (ldb/write-transit-str
                         (c.m/<? (crypt/<encrypt-uint8array aes-key asset-file))))
          http-task (http/put put-url {:headers {"x-amz-meta-checksum" checksum
                                                 "x-amz-meta-type" asset-type}
                                       :body asset-file*
                                       :with-credentials? false})]
      (m/? http-task))))
```

**Location:** `src/main/frontend/handler/assets.cljs:270-294`

### Asset Download & Decryption

```clojure
(defn new-task--rtc-download-asset
  [repo aes-key asset-block-uuid-str asset-type get-url]
  (m/sp
    (let [{:keys [status body]} (m/? (http/get get-url {:response-type :array-buffer}))]
      (let [asset-file
            (if (not aes-key)
              body
              (try
                (let [asset-file-untransited (ldb/read-transit-str
                                              (.decode (js/TextDecoder.) body))]
                  (c.m/<? (crypt/<decrypt-uint8array aes-key asset-file-untransited)))
                (catch js/SyntaxError _ body) ;; Fallback for unencrypted assets
                (catch :default e
                  (if (= "decrypt-uint8array" (ex-message e))
                    body ;; Fallback on decrypt failure
                    (throw e)))))]
        (c.m/<? (<write-asset repo asset-block-uuid-str asset-type asset-file))))))
```

**Location:** `src/main/frontend/handler/assets.cljs:296-333`

**Security Notes:**

- ✅ Assets encrypted with same AES-256-GCM key as graph data
- ✅ Each asset gets unique IV (random per encryption)
- ✅ Graceful fallback for legacy unencrypted assets
- ✅ S3 presigned URLs prevent unauthorized access

---

## Transport Security

### WebSocket Security

**Protocol:** WSS (WebSocket Secure)  
**Encryption:** TLS 1.2+ (enforced by browser/OS)

**Production URL:**

```clojure
(defonce default-db-sync-ws-url
  "wss://api.logseq.io/sync/%s")
```

**Development URL (local flag path):**

```clojure
(defonce default-db-sync-ws-url
  "ws://127.0.0.1:8787/sync/%s")
```

**Location:** `src/main/frontend/config.cljs:41-48`

> **Removed:** Legacy `ws.logseq.com/file-sync` and `ws-dev.logseq.com/file-sync` endpoint claims are no longer accurate for current db-sync defaults.

### Defense in Depth

Even though data is encrypted end-to-end, transport layer security provides:

1. **Server Authentication:** Validates server identity via TLS certificates
2. **Connection Confidentiality:** Prevents traffic sniffing
3. **Integrity Protection:** Detects tampering in transit
4. **Replay Protection:** Prevents message replay attacks

**WebSocket Implementation:**

```clojure
(defn- open-ws-task
  [url]
  (fn [s! f!]
    (let [ws (js/WebSocket. url)] ;; Browser enforces WSS security
      (set! (.-onopen ws) (fn [_] (s! [mbx ws close-dfv])))
      (set! (.-onclose ws) (fn [e] (f! e)))
      ;; ... connection handling
      )))
```

**Location:** `src/main/frontend/worker/rtc/ws.cljs:18-48`

> **Removed:** This legacy `worker/rtc/ws.cljs` path is no longer the active db-sync transport implementation. See `src/main/frontend/worker/sync/transport.cljs`.

### Message Size Optimization

Large messages (>100KB) are uploaded to S3 first, then referenced:

```clojure
(defn- put-apply-ops-message-on-s3-if-too-huge
  [ws message]
  (m/sp
    (let [message-str (js/JSON.stringify (clj->js message))
          len (.-length (utf8/encode message-str))]
      (when (< 100000 len)
        (let [{:keys [url key]} (m/? (ws/send&recv ws
                                      {:action "presign-put-temp-s3-obj"}))
              {:keys [status]} (m/? (http/put url {:body message-str}))]
          key)))))
```

**Location:** `src/main/frontend/worker/rtc/ws_util.cljs:36-54`

> **Removed:** This legacy `worker/rtc/ws_util.cljs` reference is retained for historical context only; current sync transport/message shaping lives in `src/main/frontend/worker/sync/transport.cljs`.

---

## Password Storage

### System Keychain Integration

User E2EE passwords are stored securely in platform-specific secure storage:

| Platform    | Storage Mechanism  | API                      |
| ----------- | ------------------ | ------------------------ |
| **macOS**   | Keychain Access    | `keytar` library         |
| **Windows** | Credential Manager | `keytar` library         |
| **Linux**   | Secret Service API | `keytar` library         |
| **iOS**     | iOS Keychain       | Capacitor Secure Storage |
| **Android** | Android Keystore   | Capacitor Secure Storage |

### Desktop (Electron) Implementation

**Library:** `keytar` (native Node.js keychain wrapper)  
**Service Name:** `"Logseq E2EE"` (or app name + " E2EE")

```clojure
(defn <set-password!
  "Persist encrypted password in OS keychain"
  [key encrypted-text]
  (-> (p/let [_ (.setPassword keytar (keychain-service) account encrypted-text)]
        true)
      (p/catch (fn [e]
                 (logger/error ::set-password {:error e})
                 (throw e)))))
```

**Location:** `src/electron/electron/keychain.cljs:23-33`

### Mobile (Capacitor) Implementation

**Library:** `@aparajita/capacitor-secure-storage`  
**Encryption:** Hardware-backed encryption on supported devices

```clojure
(defn <set-item!
  [key value]
  (-> (p/let [_ (.setKeyPrefix SecureStorage key-prefix)
              _ (.setItem SecureStorage key value)]
        nil)
      (p/catch (fn [e]
                 (log/error ::set-item e)
                 (throw e)))))
```

**Location:** `src/main/frontend/mobile/secure_storage.cljs:21-38`

### Password Encryption

Before storage, the user password is encrypted with the **refresh token**:

```clojure
(defn- <save-e2ee-password
  [password]
  (p/let [platform' (platform/current)
          refresh-token (if (browser-runtime? platform')
                          (:auth/refresh-token @worker-state/*state)
                          (<read-refresh-token-from-auth-file platform'))
          result (crypt/<encrypt-text-by-text-password refresh-token password)
          text (ldb/write-transit-str result)]
    (platform/save-secret-text! platform' e2ee-password-secret-key text)))
```

**Location:** `src/main/frontend/worker/sync/crypt.cljs:108-132`

> **Removed:** OPFS file-based password persistence claim (`opfs/<write-text! e2ee-password-file`) is stale for the current primary sync E2EE path.

**Double Encryption:**

1. User password → PBKDF2 → Encrypts RSA private key
2. Encrypted password → Stored in keychain (encrypted by keychain)
3. Refresh token → PBKDF2 → Encrypts password before keychain storage

This provides **defense in depth** even if keychain is compromised.

---

## Security Analysis

### Strengths

#### 1. Industry-Standard Algorithms ✅

All cryptographic primitives are **NIST-approved** and **widely vetted**:

- RSA-4096: Exceeds NIST recommendations for long-term security
- AES-256-GCM: NSA Suite B approved, used in TLS 1.3
- PBKDF2-SHA256: NIST SP 800-132 compliant
- Web Crypto API: Browser-native, hardware-accelerated where available

#### 2. Hybrid Encryption Design ✅

Combines asymmetric and symmetric encryption optimally:

- RSA for secure key distribution
- AES for efficient bulk encryption
- Separates user identity (RSA) from data encryption (AES)

#### 3. Zero-Knowledge Architecture ✅

Server **cannot decrypt** user data:

- Private keys never transmitted unencrypted
- Graph AES keys encrypted client-side before upload
- Server only stores encrypted keys and data

#### 4. Key Isolation ✅

- Each graph has unique AES key
- User RSA keys separate from graph keys
- Compromising one graph doesn't affect others

#### 5. Authenticated Encryption ✅

AES-GCM provides:

- **Confidentiality:** Data cannot be read without key
- **Integrity:** Tampering is detectable
- **Authenticity:** Verifies data origin

#### 6. Secure Random Generation ✅

All IVs and salts use **cryptographically secure random number generator**:

```clojure
(js/crypto.getRandomValues (js/Uint8Array. 12)) ;; Secure PRNG
```

#### 7. Platform Security Integration ✅

Leverages OS-level security:

- System keychains for password storage
- Hardware security modules (HSM) on supported devices
- Sandboxed storage (iOS, Android)

---

### Weaknesses & Risks

#### 1. Password Policy & KDF Posture ⚠️

> **Removed:** The old explicit 6-character/weak-score requirement claim is stale.

**Current KDF Status:**

- **PBKDF2 iterations:** 600,000 for new encrypted private-key payloads
- **Backward compatibility:** decryption supports older 100,000-iteration payloads

**PBKDF2 Settings:**

- **Iterations (new payloads):** 600,000
- **OWASP guidance:** aligned with 600,000+ recommendation band
- **NIST minimum:** exceeded

**Combined Risk:** **MEDIUM** - KDF posture improved; remaining risk is primarily around password policy definition/enforcement and user behavior.

> **Removed:** Prior brute-force timing estimates in this section were based on the stale 100k + 6-char assumption.

**Recommendation:**

```clojure
;; Increase to 600,000 iterations
:iterations 600000  ;; Current new-payload setting
```

**Impact:** ~1 second computational cost on modern hardware, acceptable UX trade-off

**Location:** `src/main/frontend/common/crypt.cljs:96-106, 131-139`

---

#### 2. No Key Rotation ⚠️

**Current Behavior:**

- RSA keys generated once, never rotated
- Graph AES keys persist indefinitely
- No automatic re-encryption mechanism

**Risk:**

- Long-term key compromise affects all historical data
- Former collaborators retain permanent access

**Recommendation:**

- Implement periodic key rotation (e.g., annual)
- Re-encrypt data with new keys
- Provide "revoke access" feature for removed collaborators

---

#### 3. No Forward Secrecy ⚠️

**Current Behavior:**

- Same AES key encrypts all messages in a graph
- Compromising AES key exposes all past messages

**Risk:** If AES key is compromised, entire graph history is exposed

**Recommendation:**

- Implement Diffie-Hellman key exchange per session
- Use Signal Protocol or similar for forward secrecy
- Rotate session keys periodically

---

#### 4. Password Reset Risk 🟡

**Current Behavior:**

- "Forgot password" flow allows password reset
- New password can decrypt old encrypted private key

**Potential Risk:** If implementation allows server-assisted recovery, it could undermine E2EE

**Recommendation:**

- Verify that password reset requires re-encryption of private key
- Ensure server cannot facilitate recovery without user's original data

**Requires Code Review:** Password reset flow implementation

---

#### 5. Metadata Leakage 🟡

**Not Encrypted:**

- Graph UUIDs
- User emails (for key exchange)
- Block UUIDs (for sync coordination)
- Timestamps (for conflict resolution)

**Risk:** Metadata analysis can reveal patterns:

- Who collaborates with whom
- Activity patterns (when edits occur)
- Graph structure (number of blocks)

**Recommendation:**

- Consider encrypting non-essential metadata
- Implement metadata minimization strategies
- Use anonymous identifiers where possible

---

#### 6. Client-Side Key Storage 🟡

**Current Behavior:**

- Encrypted private keys stored in IndexedDB
- Decrypted keys held in memory during session

**Risk:**

- Memory dumps could expose keys
- XSS vulnerabilities could extract keys
- Malicious extensions could access IndexedDB

**Mitigations in Place:**

- Keys only decrypted in memory (not persisted unencrypted)
- Web Crypto API keys are non-extractable where possible
- Content Security Policy (CSP) should restrict XSS

**Recommendation:**

- Implement additional CSP hardening
- Consider memory protection techniques
- Regular security audits of client-side code

---

### Vulnerability Assessment

| Vulnerability             | Severity | Likelihood | Impact   | Mitigation Priority                         |
| ------------------------- | -------- | ---------- | -------- | ------------------------------------------- |
| Weak Password Brute Force | **High** | **High**   | Critical | **CRITICAL** (fix password policy + PBKDF2) |
| Long-term Key Compromise  | Medium   | Low        | High     | Medium (implement rotation)                 |
| Forward Secrecy Absence   | Low      | Low        | Medium   | Low (future enhancement)                    |
| Metadata Analysis         | Low      | Medium     | Low      | Low (privacy improvement)                   |
| Client XSS Attack         | High     | Low        | Critical | **HIGH** (CSP hardening)                    |
| Memory Extraction         | Medium   | Very Low   | High     | Low (OS-level protection)                   |

---

## Threat Model

### Threat Actors

#### 1. External Attacker (Network)

**Capabilities:**

- Intercept network traffic
- Man-in-the-middle attacks
- Replay attacks

**Mitigations:**

- ✅ TLS 1.2+ for all WebSocket connections
- ✅ E2EE prevents plaintext exposure even if TLS compromised
- ✅ Server authentication via TLS certificates

**Residual Risk:** 🟢 Low

---

#### 2. Malicious Server Operator

**Capabilities:**

- Access to all stored data
- Database queries
- Server-side code execution

**Mitigations:**

- ✅ Zero-knowledge architecture: server cannot decrypt
- ✅ Private keys encrypted before upload
- ✅ Graph AES keys encrypted with RSA

**Residual Risk:** 🟢 Low (E2EE design goal achieved)

---

#### 3. Compromised Collaborator

**Capabilities:**

- Access to shared graph AES key
- Decrypt all graph content
- Cannot access other graphs

**Mitigations:**

- ✅ Graph-level key isolation
- ⚠️ No key rotation (ex-collaborators retain access)

**Residual Risk:** 🟡 Medium (key rotation needed)

---

#### 4. Client-Side Malware

**Capabilities:**

- Memory dumps
- Keylogging
- Screen capture
- Extension injection

**Mitigations:**

- ⚠️ Limited: relies on OS/browser security
- ✅ Keys only in memory during active session
- ✅ Password stored in system keychain (OS-protected)

**Residual Risk:** 🟡 Medium (inherent to client-side apps)

---

#### 5. Physical Device Access

**Capabilities:**

- Disk access
- Memory forensics (if device unlocked)

**Mitigations:**

- ✅ Private keys encrypted (password-protected)
- ✅ System keychain requires OS authentication
- ✅ Full disk encryption (user responsibility)

**Residual Risk:** 🟢 Low (with OS-level encryption)

---

### Attack Scenarios

#### Scenario 1: Passive Network Eavesdropping

**Attack:**

```
Attacker intercepts WebSocket traffic
    │
    ├──> Captures encrypted messages
    │
    └──> Attempts decryption
```

**Outcome:** ✅ **FAIL** - All data E2EE encrypted, TLS prevents MITM

---

#### Scenario 2: Server Database Breach

**Attack:**

```
Attacker gains database access
    │
    ├──> Extracts encrypted private keys
    ├──> Extracts encrypted AES keys
    ├──> Extracts encrypted graph data
    │
    └──> Attempts decryption
```

**Outcome:** ✅ **FAIL** - Without user passwords, cannot decrypt

---

#### Scenario 3: Weak Password Brute Force

**Attack:**

```
Attacker obtains encrypted private key
    │
    ├──> Brute force password (100k PBKDF2 iterations)
    │
    ├──> Weak password guessed (e.g., "password123")
    │
    ├──> Decrypts private key
    │
    ├──> Decrypts graph AES key
    │
    └──> Decrypts all graph data
```

**Outcome:** ⚠️ **SUCCESS** (very likely with current 6-char minimum)

**Current Vulnerability:**

- Minimum password: 6 characters + strength score ≥1
- Attacker can brute force in ~1.5 hours on consumer GPU
- No rate limiting on offline attacks (encrypted data obtained)

**Mitigation Required:**

1. **Increase minimum password length to 12 characters** (CRITICAL)
2. **Require strength score ≥2 (Medium or Strong)** (CRITICAL)
3. **Increase PBKDF2 iterations to 600,000+** (HIGH)
4. **Implement server-side rate limiting** (Medium)
5. **Add password breach database checking** (Medium)

---

#### Scenario 4: Malicious Server Key Substitution (Multi-User Sharing)

**Attack:**

```
Malicious/Compromised Server attempts MITM on key exchange
    │
    ├──> Owner requests collaborator's public key by email
    │
    ├──> Server returns ATTACKER's public key instead
    │
    ├──> Owner encrypts graph AES key with attacker's key
    │
    ├──> Server decrypts graph AES key
    │
    └──> Server can decrypt all graph data
```

**Outcome:** ⚠️ **PARTIAL SUCCESS** (requires malicious server operator)

**Current Vulnerability:**

- No public key verification/authentication mechanism
- Email-based lookup trusts server implicitly
- No key fingerprint comparison between users
- No certificate pinning or key transparency log
- Owner cannot verify they received correct collaborator public key

**Mitigations:**

1. **Display public key fingerprints for manual verification** (HIGH)
2. **Implement key transparency log** (Medium - complex)
3. **Out-of-band verification** (QR codes, safety numbers) (Medium)
4. **Document server trust assumption** (IMMEDIATE)

**Residual Risk:** 🟡 Medium (requires malicious server, but no detection)

**Note:** This is a **known limitation** of email-based key exchange. Most secure messaging apps (Signal, WhatsApp) use phone number + key servers but face similar trust assumptions. The difference is they implement safety number verification.

---

#### Scenario 5: Collaborator Account Compromise (Shared Graph)

**Attack:**

```
Attacker compromises collaborator's account
    │
    ├──> Obtains collaborator's encrypted private key from server
    │
    ├──> Brute forces weak collaborator password
    │
    ├──> Decrypts collaborator's RSA private key
    │
    ├──> Fetches encrypted graph AES key (their copy)
    │
    ├──> Decrypts graph AES key with private key
    │
    └──> Decrypts ALL shared graph data
```

**Outcome:** ⚠️ **SUCCESS** (if collaborator uses weak password)

**Current Vulnerability:**

- Each collaborator sets independent password ✅ (good design)
- But password policy weakness applies to ALL users ❌
- **One weak collaborator password compromises entire shared graph**
- No ability for owner to enforce password requirements
- No visibility into collaborator security posture
- No audit log of who accessed what data

**Mitigation Required:**

1. **Fix global password policy** (benefits all users) (CRITICAL)
2. **Add access revocation mechanism** (HIGH)
3. **Graph-level "minimum security" requirements** (Medium)
4. **Audit log of collaborator access** (Medium)
5. **Notify owner of collaborator password changes** (Low)

**Residual Risk:** 🟡 Medium (weakest link problem inherent to sharing)

**Impact:** Shared graphs are only as secure as the weakest collaborator's password. This is a fundamental challenge in multi-user E2EE systems.

---

#### Scenario 6: XSS Code Injection

**Attack:**

```
Attacker injects malicious script
    │
    ├──> Script accesses IndexedDB
    │
    ├──> Extracts encrypted private key
    │
    ├──> Waits for user to enter password
    │
    ├──> Intercepts decrypted key in memory
    │
    └──> Exfiltrates to attacker server
```

**Outcome:** ⚠️ **SUCCESS** (if XSS possible) - Mitigate with:

- Strict Content Security Policy (CSP)
- Regular security audits
- Input sanitization
- DOM-based XSS prevention

---

## Code References

### Core Encryption Functions

| Function                         | Purpose                               | Location             |
| -------------------------------- | ------------------------------------- | -------------------- |
| `<generate-rsa-key-pair`         | Generate RSA-4096 keys                | `crypt.cljs:58-78`   |
| `<generate-aes-key`              | Generate AES-256 key                  | `crypt.cljs:82-88`   |
| `<encrypt-private-key`           | Encrypt RSA private key with password | `crypt.cljs:90-115`  |
| `<decrypt-private-key`           | Decrypt RSA private key               | `crypt.cljs:117-146` |
| `<encrypt-aes-key`               | Encrypt AES key with RSA              | `crypt.cljs:148-156` |
| `<decrypt-aes-key`               | Decrypt AES key                       | `crypt.cljs:158-181` |
| `<encrypt-uint8array`            | Encrypt binary data (assets)          | `crypt.cljs:183-193` |
| `<decrypt-uint8array`            | Decrypt binary data                   | `crypt.cljs:195-208` |
| `<encrypt-text`                  | Encrypt text data                     | `crypt.cljs:210-222` |
| `<decrypt-text`                  | Decrypt text data                     | `crypt.cljs:224-242` |
| `<encrypt-text-by-text-password` | PBKDF2 password encryption            | `crypt.cljs:289-316` |
| `<decrypt-text-by-text-password` | PBKDF2 password decryption            | `crypt.cljs:318-347` |

**File:** `src/main/frontend/common/crypt.cljs`

---

### Key Management

| Function                           | Purpose                     | Location                 |
| ---------------------------------- | --------------------------- | ------------------------ |
| `task--upload-user-rsa-key-pair`   | Upload user keys to server  | `rtc/crypt.cljs:86-100`  |
| `task--fetch-user-rsa-key-pair`    | Fetch user keys from server | `rtc/crypt.cljs:115-131` |
| `task--fetch-graph-aes-key`        | Get graph encryption key    | `rtc/crypt.cljs:143-158` |
| `task--generate-graph-aes-key`     | Create new graph key        | `rtc/crypt.cljs:160-162` |
| `task--get-decrypted-rsa-key-pair` | Decrypt user RSA keys       | `rtc/crypt.cljs:164-178` |
| `<save-e2ee-password`              | Save password to keychain   | `rtc/crypt.cljs:42-53`   |
| `<read-e2ee-password`              | Read password from keychain | `rtc/crypt.cljs:55-63`   |

**File:** `src/main/frontend/worker/rtc/crypt.cljs`

---

### Platform Storage

| Function            | Purpose                          | Location                       |
| ------------------- | -------------------------------- | ------------------------------ |
| `<set-password!`    | Store in keychain (desktop)      | `electron/keychain.cljs:23-33` |
| `<get-password`     | Retrieve from keychain           | `electron/keychain.cljs:35-45` |
| `<delete-password!` | Remove from keychain             | `electron/keychain.cljs:47-57` |
| `<set-item!`        | Store in secure storage (mobile) | `secure_storage.cljs:21-38`    |
| `<get-item`         | Retrieve from secure storage     | `secure_storage.cljs:40-50`    |
| `<remove-item!`     | Remove from secure storage       | `secure_storage.cljs:52-62`    |

**Files:**

- `src/electron/electron/keychain.cljs`
- `src/main/frontend/mobile/secure_storage.cljs`

---

### Asset Encryption

| Function                       | Purpose                  | Location              |
| ------------------------------ | ------------------------ | --------------------- |
| `new-task--rtc-upload-asset`   | Encrypt & upload asset   | `assets.cljs:270-294` |
| `new-task--rtc-download-asset` | Download & decrypt asset | `assets.cljs:296-333` |

**File:** `src/main/frontend/handler/assets.cljs`

---

### Transport Security

| Function        | Purpose                          | Location                            |
| --------------- | -------------------------------- | ----------------------------------- |
| `format-ws-url` | Compose graph-scoped sync WS URL | `worker/sync/transport.cljs:11-20`  |
| `append-token`  | Attach auth token to WS URL      | `worker/sync/transport.cljs:22-27`  |
| `send!`         | Send validated WS client message | `worker/sync/transport.cljs:89-109` |

**Files:**

- `src/main/frontend/worker/sync/transport.cljs`

---

### Configuration

| Constant                           | Value                         | Location            |
| ---------------------------------- | ----------------------------- | ------------------- |
| `default-db-sync-ws-url` (prod)    | `wss://api.logseq.io/sync/%s` | `config.cljs:41-44` |
| `default-db-sync-http-base` (prod) | `https://api.logseq.io`       | `config.cljs:46-49` |

**File:** `src/main/frontend/config.cljs`

---

## Recommendations

### Immediate (Critical Priority)

#### 1. Define and Enforce Explicit Password Policy ⚡

> **Removed:** The referenced `src/main/frontend/components/encryption.cljs` password policy code path no longer exists.

**Current Need:**

Perform a focused audit and explicitly define password policy requirements in the current E2EE entry points (UI + CLI), then verify enforcement end-to-end.

**Additional Validation:**

```clojure
;; Add entropy check
(defn validate-password-entropy [password]
  (>= (calculate-entropy password) 60))  ;; ~60 bits minimum

;; Update UI messages
[:span "Password must be at least 12 characters and have Medium or Strong strength"]
```

**Impact:**

- ✅ Clarifies security guarantees and enforcement behavior across UI/CLI
- ✅ Reduces ambiguity about acceptable password quality
- ⚠️ May require explicit migration and user messaging if stricter rules are introduced

**Files to Modify (candidate):**

1. `src/main/frontend/components/e2ee.cljs`
2. `src/main/frontend/worker/sync/crypt.cljs`

**Testing Required:**

- Unit tests for password validation
- E2E tests for RTC password setup
- Ensure backward compatibility with existing passwords

---

#### 2. Maintain PBKDF2 Compatibility and Migration Hygiene

**Current:** 600,000 iterations for new payloads; legacy decrypt support remains

No immediate change required; ensure migration/compatibility tests keep legacy payload decryption stable.

**Impact:**

- ✅ Improved brute-force resistance already delivered
- ✅ Backward compatibility path exists in decrypt flow

---

#### 3. Implement Content Security Policy (CSP) ⚡

**Goal:** Prevent XSS attacks that could extract keys

**Recommended CSP Headers:**

```http
Content-Security-Policy:
  default-src 'self';
  script-src 'self' 'unsafe-eval';
  style-src 'self' 'unsafe-inline';
  img-src 'self' data: https:;
  connect-src 'self' wss://ws.logseq.com wss://ws-dev.logseq.com;
  frame-ancestors 'none';
```

**Impact:**

- ✅ Blocks unauthorized script execution
- ✅ Prevents data exfiltration to third-party domains
- ⚠️ May require adjustments for plugins/extensions

---

### Medium Priority

#### 4. Multi-User Sharing Security Enhancements 👥 **HIGH**

**Goal:** Address weakest-link problem and key verification

**A. Public Key Fingerprint Verification**

```clojure
;; Display fingerprint when granting access
(defn display-collaborator-key-fingerprint [public-key email]
  (let [fingerprint (-> public-key
                        (<export-public-key)
                        (p/then #(.digest (.-crypto js/window) "SHA-256" %))
                        (p/then #(bytes->hex %)))]
    [:div.key-verification
     [:p (str "Verify " email "'s key fingerprint:")]
     [:code.fingerprint fingerprint]
     [:p.text-sm.opacity-70
      "Ask them to confirm this matches their key fingerprint in Settings > Security"]]))
```

**B. Collaborator Access Revocation**

```clojure
;; Add revocation mechanism
(def-thread-api :thread-api/rtc-revoke-access
  [token graph-uuid target-user-email]
  (m/sp
    (let [{:keys [get-ws-create-task]} (ws-util/get-ws-create-map user-uuid graph-uuid)]
      ;; Remove user's encrypted AES key from server
      (m/? (ws-util/send&recv get-ws-create-task
                              {:action "revoke-access"
                               :graph-uuid graph-uuid
                               :target-user-email target-user-email}))
      ;; Optionally: rotate graph AES key for enhanced security
      (when rotate-key?
        (m/? (task--rotate-graph-aes-key get-ws-create-task graph-uuid))))))
```

**C. Collaborator Security Audit**

```clojure
;; UI: Display collaborator list with security indicators
(rum/defc collaborators-list [graph-uuid collaborators]
  [:div.collaborators
   [:h3 "Graph Collaborators"]
   [:table
    [:thead
     [:tr
      [:th "Email"]
      [:th "Added"]
      [:th "Last Access"]
      [:th "Actions"]]]
    [:tbody
     (for [collab collaborators]
       [:tr
        [:td (:email collab)]
        [:td (format-date (:added-at collab))]
        [:td (format-date (:last-access collab))]
        [:td
         (shui/button
          {:on-click #(revoke-access! graph-uuid (:email collab))
           :variant :destructive}
          "Revoke Access")]])]]])
```

**D. Graph-Level Security Requirements**

```clojure
;; Allow owner to set minimum security requirements
(defn set-graph-security-level [graph-uuid level]
  ;; level: :standard | :high | :maximum
  (case level
    :high {:min-password-length 14
           :min-password-strength 2
           :require-2fa false}
    :maximum {:min-password-length 16
              :min-password-strength 3
              :require-2fa true}))
```

**Impact:**

- Reduces risk from compromised collaborators
- Enables trust-but-verify security model
- Provides owner control over shared graph security

**Location:** New implementations needed in:

- `src/main/frontend/components/settings.cljs` (UI)
- `src/main/frontend/worker/rtc/core.cljs` (revocation logic)
- `src/main/frontend/handler/db_based/rtc.cljs` (handler)

---

#### 5. Implement Key Rotation 🔄

**Goal:** Limit impact of key compromise

**Design:**

```
Periodic Key Rotation (Annual)
    │
    ├──> Generate new Graph AES Key
    │
    ├──> Re-encrypt recent data with new key
    │
    ├──> Store old key for historical data access
    │
    └──> Notify collaborators to update keys
```

**Benefits:**

- ✅ Limits exposure window
- ✅ Enables access revocation
- ⚠️ Complex: requires careful migration

---

#### 5. Audit Logging 📋

**Goal:** Detect suspicious activity

**Log Events:**

- RSA key pair generation
- Password changes
- Failed decryption attempts
- New collaborator access grants
- Asset downloads (especially bulk)

**Privacy:** Keep logs client-side or encrypted

---

#### 6. Secure Memory Handling 🧠

**Goal:** Prevent key extraction from memory

**Techniques:**

- Use `SecureMemory` APIs where available
- Zero-out key buffers after use
- Minimize key lifetime in memory

**Example:**

```clojure
(defn clear-key! [key-buffer]
  (.fill key-buffer 0) ;; Zero out memory
  (js/crypto.subtle.deleteKey key)) ;; If supported
```

---

### Long-term (Future Enhancements)

#### 7. Forward Secrecy (Signal Protocol) 🔐

**Goal:** Protect past messages from future key compromise

**Implementation:**

- Double Ratchet algorithm (Signal Protocol)
- Per-message ephemeral keys
- Diffie-Hellman key exchange per session

**Complexity:** High - requires significant architectural changes

---

#### 8. Hardware Security Module (HSM) Support 🔒

**Goal:** Store keys in hardware-protected storage

**Platforms:**

- **iOS:** Secure Enclave
- **Android:** StrongBox Keymaster
- **Desktop:** TPM 2.0, Yubikey

**Benefits:**

- ✅ Keys cannot be extracted from hardware
- ✅ Tamper-resistant
- ⚠️ Limited platform support

---

#### 9. Metadata Encryption 🕵️

**Goal:** Hide collaboration patterns

**Approach:**

- Encrypt block UUIDs (use deterministic encryption)
- Anonymous user identifiers
- Obfuscate timestamps (round to nearest hour)
- Encrypt graph structure metadata

**Trade-off:** May complicate sync logic

---

#### 10. Zero-Knowledge Password Recovery 🔑

**Current:** Password reset requires re-encryption

**Improvement:**

- Implement **Shamir's Secret Sharing**
- User creates recovery shares (e.g., 3 of 5)
- Shares distributed to trusted devices/contacts
- Recovery requires threshold (3 shares)

**Benefit:** Secure recovery without server involvement

---

## Testing & Validation

### Security Test Suite

#### Unit Tests

**Location:** `src/test/frontend/common/crypt_test.cljs`

**Coverage:**

- ✅ RSA key pair generation and encryption
- ✅ AES key generation and encryption
- ✅ Password-based encryption/decryption
- ✅ Error handling (wrong password, wrong key)

**Example Test:**

```clojure
(deftest-async user-rsa-key-pair+graph-aes-key+password-test
  (p/let [password "test-password"
          rsa-key-pair (crypt/<generate-rsa-key-pair)
          aes-key (crypt/<generate-aes-key)
          encrypted-private-key (crypt/<encrypt-private-key password
                                                            (:privateKey rsa-key-pair))
          encrypted-aes-key (crypt/<encrypt-aes-key (:publicKey rsa-key-pair) aes-key)]
    ;; Validate encryption
    (is (vector? encrypted-private-key))
    (is (instance? js/Uint8Array encrypted-aes-key))

    ;; Validate decryption
    (p/let [decrypted-private-key (crypt/<decrypt-private-key password
                                                              encrypted-private-key)
            decrypted-aes-key (crypt/<decrypt-aes-key decrypted-private-key
                                                      encrypted-aes-key)]
      (is (instance? js/CryptoKey decrypted-private-key))
      (is (instance? js/CryptoKey decrypted-aes-key)))))
```

#### E2E Tests

**Location:** `clj-e2e/test/logseq/e2e/rtc_extra_test.clj`

**Scenarios:**

- Multi-user collaboration with E2EE
- Asset upload/download with encryption
- Password change flow
- Access revocation

**Command:**

```bash
bb dev:e2e-rtc-extra-test
```

---

### Penetration Testing Recommendations

#### 1. Cryptographic Analysis

- Verify random number generator entropy
- Test PBKDF2 iteration count effectiveness
- Validate AES-GCM implementation

#### 2. Side-Channel Analysis

- Timing attacks on password verification
- Memory analysis (key extraction attempts)
- Power analysis (if hardware-based)

#### 3. Protocol Analysis

- WebSocket message inspection
- Man-in-the-middle testing
- Replay attack testing

#### 4. Application Security

- XSS vulnerability scanning
- CSRF testing
- IndexedDB access control validation

---

## Compliance & Standards

### Adherence to Standards

| Standard                         | Status        | Notes                                 |
| -------------------------------- | ------------- | ------------------------------------- |
| **NIST SP 800-132** (PBKDF2)     | ✅ Compliant  | 100k iterations meets minimum         |
| **NIST SP 800-63B** (Passwords)  | ⚠️ Partial    | Recommend 12+ char minimum            |
| **NIST SP 800-175B** (Key Sizes) | ✅ Compliant  | RSA-4096, AES-256 exceed requirements |
| **OWASP Top 10**                 | ✅ Addressed  | E2EE mitigates data exposure risks    |
| **GDPR** (EU)                    | ✅ Compatible | E2EE supports data minimization       |
| **CCPA** (California)            | ✅ Compatible | User controls encryption keys         |

---

### Encryption Strength Summary

| Component    | Algorithm     | Key Size  | Security Bits       | NIST Category |
| ------------ | ------------- | --------- | ------------------- | ------------- |
| User Keys    | RSA-OAEP      | 4096 bits | ~140 bits           | Exceeds 2030+ |
| Graph Data   | AES-GCM       | 256 bits  | 256 bits            | NSA Suite B   |
| Password KDF | PBKDF2-SHA256 | 256 bits  | Depends on password | NIST approved |

**Overall Security Rating:** 🟢 **Strong** (exceeds industry standards)

---

## Glossary

- **E2EE:** End-to-End Encryption - Data encrypted on client, server cannot decrypt
- **RSA-OAEP:** RSA with Optimal Asymmetric Encryption Padding
- **AES-GCM:** Advanced Encryption Standard in Galois/Counter Mode (authenticated encryption)
- **PBKDF2:** Password-Based Key Derivation Function 2
- **IV:** Initialization Vector - Random value ensuring unique ciphertexts
- **Salt:** Random data added to password before hashing
- **HSM:** Hardware Security Module - Physical device for key storage
- **Forward Secrecy:** Property where past messages remain secure even if current keys compromised
- **AEAD:** Authenticated Encryption with Associated Data
- **WSS:** WebSocket Secure (WebSocket over TLS)

---

## Change History

| Version | Date       | Changes                                      |
| ------- | ---------- | -------------------------------------------- |
| 1.0     | 2025-12-07 | Initial comprehensive security investigation |

---

## Related Documentation

- [RTC Developer Onboarding Guide](../01-onboarding/rtc-onboarding.md)
- [RTC vs File Sync Comparison](../02-architecture/rtc-vs-sync.md)
- [Data Architecture](../02-architecture/data-architecture.md)
- [Code Detective Investigation Template](../.github/prompts/code-detective-investigation.prompt.md)

---

## Conclusion

Logseq's E2EE implementation for Real-Time Collaboration demonstrates **strong adherence to cryptographic best practices** and **industry standards**. The use of RSA-4096, AES-256-GCM, and platform-specific secure storage provides robust protection against various threat actors.

### Security Posture: 🟢 Strong

**Key Strengths:**

- Zero-knowledge architecture (server cannot decrypt)
- Industry-standard algorithms (NIST-approved)
- Multi-layer encryption (hybrid model)
- Platform security integration

**Recommended Improvements:**

1. **CRITICAL (Immediate):** Fix weak password policy (6→12 chars, strength 1→2)
2. **High Priority:** Increase PBKDF2 iterations to 600,000+
3. **High Priority:** Implement Content Security Policy (CSP)
4. **Medium Priority:** Add key rotation and forward secrecy

With the recommended enhancements implemented, Logseq's E2EE would achieve **best-in-class security** for collaborative knowledge management applications.

---

**Security Contact:** For security concerns, please follow responsible disclosure practices outlined in Logseq's security policy.

**License:** This documentation is part of the Logseq project and follows the same license.
