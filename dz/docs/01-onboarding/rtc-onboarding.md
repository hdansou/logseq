# Logseq Real-Time Collaboration (RTC) Developer Onboarding Guide

> **Last revised:** 2026-05-28

Welcome to the Logseq RTC development team! This comprehensive guide will help you understand the RTC architecture, codebase structure, and how to contribute effectively to the real-time collaboration features.

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Codebase Structure](#codebase-structure)
4. [Key Concepts](#key-concepts)
5. [Development Setup](#development-setup)
6. [Common Development Tasks](#common-development-tasks)
7. [Testing](#testing)
8. [Debugging Guide](#debugging-guide)
9. [Contributing Guidelines](#contributing-guidelines)
10. [Resources](#resources)

## Overview

Logseq's Real-Time Collaboration (RTC) enables multiple users to edit the same graph simultaneously with real-time synchronization. Unlike traditional CRDT-based systems, Logseq uses an **operation-based approach** with **fractional indexing** and **centralized conflict resolution**.

### Key Features

- **Real-time synchronization** of block edits, moves, and deletions
- **Conflict resolution** using transaction ordering
- **User presence awareness** showing who's online
- **Fractional indexing** for block ordering without conflicts
- **Asset synchronization** for images and files

## Architecture

### High-Level Architecture

```txt
┌─────────────────┐   WebSocket    ┌─────────────────┐   WebSocket    ┌─────────────────┐
│   Client A      │◄──────────────►│     Server      │◄──────────────►│   Client B      │
│                 │                │                 │                │                 │
│ ┌─────────────┐ │                │ ┌─────────────┐ │                │ ┌─────────────┐ │
│ │ Local Ops   │ │                │ │ Conflict    │ │                │ │ Local Ops   │ │
│ │ Queue       │ │                │ │ Resolution  │ │                │ │ Queue       │ │
│ └─────────────┘ │                │ └─────────────┘ │                │ └─────────────┘ │
│ ┌─────────────┐ │                │ ┌─────────────┐ │                │ ┌─────────────┐ │
│ │ DataScript  │ │                │ │ Operation   │ │                │ │ DataScript  │ │
│ │ Database    │ │                │ │ Broadcasting│ │                │ │ Database    │ │
│ └─────────────┘ │                │ └─────────────┘ │                │ └─────────────┘ │
└─────────────────┘                └─────────────────┘                └─────────────────┘
```

### Core Components

1. **Client Operations**: Local changes captured as operations
2. **WebSocket Communication**: Real-time bidirectional messaging
3. **Conflict Resolution**: Server-side operation ordering and merging
4. **Fractional Indexing**: Block positioning without conflicts
5. **Asset Synchronization**: Handling images and file uploads

## Codebase Structure

The RTC/sync code is split across two main locations:

### Sync library: `deps/db-sync/src/logseq/db_sync/`

This is the core sync engine, extracted into its own dependency:

```txt
db_sync/
├── worker.cljs                  # Main sync worker orchestration
├── worker/
│   ├── ws.cljs                  # WebSocket handling
│   ├── auth.cljs                # Authentication
│   ├── coerce.cljs              # Data coercion
│   ├── dispatch.cljs            # Operation dispatch
│   ├── http.cljs                # HTTP transport
│   ├── presence.cljs            # User presence tracking
│   ├── timing.cljs              # Timing utilities
│   ├── routes/                  # Route handlers
│   └── handler/
│       ├── sync.cljs            # Sync handler
│       ├── ws.cljs              # WebSocket handler
│       ├── assets.cljs          # Asset sync handler
│       └── index.cljs           # Index handler
├── malli_schema.cljs            # Data validation schemas
├── batch.cljs                   # Batch operations
├── checksum.cljs                # Sync integrity checksums
├── common.cljs                  # Shared utilities
├── cycle.cljs                   # Sync cycle management
├── index.cljs                   # Indexing
├── node/                        # Server-side Node.js adapter
├── order.cljs                   # Operation ordering
├── snapshot.cljs                # Graph snapshots
├── storage.cljs                 # Storage abstraction
├── protocol.cljs                # Protocol definitions
├── logging.cljs                 # Logging
├── platform/                    # Platform adapters (node, cloudflare)
├── sentry.cljs                  # Sentry error reporting entry
├── sentry/                      # Error reporting
└── tx_sanitize.cljs             # Transaction sanitization for deleted properties
```

### Client-side sync files: `src/main/frontend/worker/sync/`

```txt
sync/
├── apply_txs.cljs               # Transaction application logic
├── asset_db_listener.cljs       # Asset change listeners
├── assets.cljs                  # Asset sync operations
├── auth.cljs                    # Authentication (client-side)
├── client_op.cljs               # Client operation storage (SQLite-based)
├── const.cljs                   # Constants and configuration
├── crypt.cljs                   # Encryption utilities
├── download.cljs                # Graph/asset download handling
├── handle_message.cljs          # WebSocket message routing
├── large_title.cljs             # Large-title handling during sync
├── log_and_state.cljs           # Logging and state management
├── presence.cljs                # Client-side user presence
├── temp_sqlite.cljs             # Temporary SQLite operations
├── transport.cljs               # Transport/coercion layer
├── upload.cljs                  # Graph/asset upload handling
└── util.cljs                    # Shared utilities
```

### Related Files

- `deps/db/src/logseq/db/common/order.cljs` - Fractional indexing utilities
- `src/main/frontend/handler/db_based/rtc_flows.cljs` - RTC flow handlers in the app
- `src/main/frontend/handler/db_based/rtc_background_tasks.cljs` - Background RTC tasks
- `src/main/frontend/handler/events/rtc.cljs` - RTC event handlers
- `src/main/frontend/components/rtc/indicator.cljs` - RTC UI indicator
- `src/main/frontend/db/rtc/debug_ui.cljs` - RTC debug UI

## Key Concepts

### 1. Operations

> **Removed:** The old tuple-based operation format (`[:update t {...}]`, `[:move t {...}]`, `[:remove t {...}]`, `[:update-page ...]`, `[:remove-page ...]`, `[:update-kv-value ...]`) no longer exists. Block/page changes are stored in SQLite as `forward-outliner-ops` / `inverse-outliner-ops` (transit-encoded) and transmitted to the server as `tx/batch` messages with transit-encoded `:tx` strings. The only ops still represented in the current `client_op.cljs` op-schema are asset operations:

```clojure
;; Current operation types (client_op.cljs op-schema)
[:update-asset t {:block-uuid uuid}]   ; Upload/update an asset
[:remove-asset t {:block-uuid uuid}]   ; Remove an asset

;; Block/page changes are stored as outliner-op transactions:
;; {:tx-id uuid, :tx "<transit-encoded-datascript-tx>",
;;  :outliner-op :keyword, :forward-outliner-ops [...], :inverse-outliner-ops [...]}
```

### 2. Transaction Ordering

Each operation has timestamps for conflict resolution:

```clojure
{:t 1001               ; Operation timestamp
 :t-before 1000        ; Previous operation timestamp
 :affected-blocks {...} ; Changed blocks
 :refed-blocks [...]   ; Referenced blocks that need to exist
}
```

### 3. Fractional Indexing

Blocks use fractional indexes for ordering. The main API is in `deps/db/src/logseq/db/common/order.cljs`:

```clojure
;; Generate a key after the current max key
(order/gen-key)

;; Generate a key between two existing keys
(order/gen-key "a0" "a2")

;; The underlying library function:
(index/generate-key-between "a0" "a2")  ; Returns "a1"
(index/generate-key-between nil "a0")   ; Returns position before "a0"
(index/generate-key-between "a0" nil)   ; Returns position after "a0"
```

### 4. Conflict Resolution Process

1. **Local operations** are queued in `client-op.cljs`
2. **Remote operations** arrive via WebSocket
3. **Transaction timestamps** determine order
4. **Local operations are updated** based on remote state
5. **Database is updated** with resolved operations

### 5. WebSocket Message Types

> **Removed:** The old format using `:action` (outgoing) and `:req-id` (incoming) fields no longer exists. The protocol now uses a `:type` string field on both sides.

```clojure
;; Outgoing client messages (ws-client-message-schema in malli_schema.cljs)
{:type "hello"    :client "<client-id>"}
{:type "presence" :editing-block-uuid "<uuid-or-nil>"}
{:type "pull"     :since <t-int>}                      ; Pull updates since t
{:type "tx/batch" :t-before <int> :txs [{:tx-id uuid :tx "<transit-str>" :outliner-op kw}]}
{:type "ping"}

;; Incoming server messages (ws-server-message-schema in malli_schema.cljs)
{:type "hello"        :t <int> :checksum "<str>"}      ; Connection established
{:type "online-users" :online-users [{:user-id str :email str ...}]}
{:type "presence"     :user-id str :editing-block-uuid "<uuid-or-nil>"}
{:type "pull/ok"      :t <int> :txs [...] :checksum "<str>"}
{:type "tx/batch/ok"  :t <int> :checksum "<str>"}
{:type "changed"      :t <int>}                        ; Remote data changed
{:type "tx/reject"    :reason "<enum>" :t <int>}       ; Transaction rejected
{:type "pong"}
{:type "error"        :message "<str>"}
```

## Development Setup

### Prerequisites

1. **Clojure/ClojureScript** development environment
2. **Node.js** for dependencies
3. **Git** for version control
4. **Editor** with Clojure support (VS Code + Calva, Emacs + CIDER, IntelliJ + Cursive)

### Local Development

1. **Clone the repository**:

   ```bash
   git clone https://github.com/logseq/logseq.git
   cd logseq
   ```

2. **Install dependencies**:

   ```bash
   pnpm install
   ```

3. **Start development server**:

   ```bash
   pnpm watch  # or pnpm dev for mobile+desktop combined watch
   ```

4. **Enable RTC in development**:
   - Set up test environment variables
   - Configure WebSocket endpoints
   - Enable debug logging

### Environment Configuration

```clojure
;; In your development config
{:rtc {:enabled true
       :debug-ws-url "wss://ws-dev.logseq.com/rtc-sync"
       :debug-logging true}}
```

## Common Development Tasks

### 1. Adding a New Operation Type

> **Removed:** `gen_client_op.cljs` and `remote_update.cljs` no longer exist. Block/page change operations are handled through the outliner transaction pipeline (`forward-outliner-ops` / `inverse-outliner-ops`) rather than explicit named operation types. Only asset operations (`:update-asset`, `:remove-asset`) use the named op-schema pattern. To add a new asset operation type, follow these steps:

1. **Define the operation schema** in `malli_schema.cljs`:

   ```clojure
   [:my-new-op
    [:catn
     [:op :keyword]
     [:t :int]
     [:value [:map [:block-uuid :uuid] [:my-data :string]]]]]
   ```

2. **Add operation storage** in `client_op.cljs` (see `add-asset-ops` for the pattern):

   ```clojure
   (defn add-my-new-ops [repo ops] ...)
   ```

3. **Handle application of remote operations** in `apply_txs.cljs`:

   ```clojure
   (defn apply-my-new-op [repo conn op-value]
     ;; Implementation here
     )
   ```

4. **Add to operation partitioning** in `client_op.cljs`

### 2. Debugging RTC Issues

1. **Enable debug logging**:

   ```clojure
   (log/info :rtc-debug data)  ; use lambdaisland.glogi, not js/console.log
   ```

2. **Inspect pending transaction count**:

   ```clojure
   (client-op/get-pending-local-tx-count repo)
   ```

3. **Check WebSocket state** via the debug UI (see `src/main/frontend/db/rtc/debug_ui.cljs`):

   ```clojure
   ;; Invoke via thread-api from the renderer process:
   (state/<invoke-db-worker :thread-api/rtc-get-debug-state)
   ```

4. **Monitor transaction flow**:

```clojure
;; Add logging in apply_txs.cljs
(log/info :rtc-transaction {:local-t local-t :remote-t remote-t})
```

### 3. Testing RTC Features

1. **Unit tests** for individual functions
2. **Integration tests** for operation flows
3. **Multi-client tests** for conflict scenarios
4. **Performance tests** for large graphs

### 4. Common Patterns

#### Reading Current State

```clojure
(let [repo (worker-state/get-current-repo)
      conn (worker-state/get-datascript-conn repo)
      local-tx (client-op/get-local-tx repo)]
  ;; Use repo, conn, local-tx
  )
```

#### Generating Operations

> **Removed:** `client-op/add-ops!` does not exist. Asset ops are stored with `add-asset-ops`; block/page changes flow through the outliner transaction pipeline automatically.

```clojure
;; Store asset operations:
(client-op/add-asset-ops repo asset-ops)
```

#### Handling Remote Updates

```clojure
(defn apply-remote-op [repo conn op-value]
  (let [block-uuid (:block-uuid op-value)]
    (when-let [block (d/entity @conn [:block/uuid block-uuid])]
      ;; Apply changes to block
      )))
```

## Testing

### Test Structure

```txt
src/test/
├── frontend/
│   ├── components/
│   │   └── rtc/
│   │       └── indicator_test.cljs
│   ├── handler/
│   │   └── db_based/
│   │       ├── rtc_flows_test.cljs
│   │       └── sync_test.cljs
│   │   └── events/
│   │       └── rtc_error_test.cljs
│   └── worker/
│       ├── db_sync_sim_test.cljs
│       ├── db_sync_test.cljs
│       ├── node_sync_test.cljs
│       └── sync/
│           ├── assets_test.cljs
│           ├── client_op_test.cljs
│           ├── crypt_test.cljs
│           ├── download_test.cljs
│           ├── restart_test.cljs
│           └── upload_test.cljs
└── logseq/cli/command/
    └── sync_test.cljs
```

### Running Tests

```bash
# Lint + all unit tests (preferred)
bb dev:lint-and-test

# Run a single test by name
bb dev:test -v frontend.worker.sync.client-op-test/my-test

# All unit tests via pnpm
pnpm test

# Node adapter tests
pnpm test:node-adapter

# App E2E tests (from clj-e2e/)
bb test

# CLI E2E tests
bb -f cli-e2e/bb.edn test --skip-build
```

### Writing Tests

```clojure
(deftest test-operation-generation
  (testing "Update operation generation"
    (let [op (gen-client-op/generate-update-op block-uuid av-coll)]
      (is (= :update (first op)))
      (is (= block-uuid (:block-uuid (last op)))))))

(deftest test-conflict-resolution
  (testing "Concurrent block updates"
    (let [local-ops [...]
          remote-ops [...]]
      ;; Test conflict resolution logic
      )))
```

## Debugging Guide

### Common Issues and Solutions

#### 1. Operations Not Synchronizing

**Symptoms**: Changes not appearing on other clients

**Debug Steps**:

1. Check WebSocket connection: `ws-state` in debug state
2. Verify operation generation: Check `client-op` queue
3. Check server response: Look for error messages
4. Verify transaction ordering: Compare local-t vs remote-t

**Common Causes**:

- WebSocket disconnection
- Invalid operation format
- Schema validation failures
- Network issues

#### 2. Conflict Resolution Issues

**Symptoms**: Unexpected merge results, lost changes

**Debug Steps**:

1. Check operation timestamps
2. Verify operation dependencies
3. Look for circular references
4. Check fractional indexing

#### 3. Performance Issues

**Symptoms**: Slow synchronization, UI freezing

**Debug Steps**:

1. Profile operation processing
2. Check for large operation batches
3. Monitor memory usage
4. Analyze WebSocket message frequency

### Debug Tools

#### Built-in Debug Functions

```clojure
;; Get current RTC state (invoke from renderer via thread-api)
(state/<invoke-db-worker :thread-api/rtc-get-debug-state)

;; Check pending local transaction count
(client-op/get-pending-local-tx-count repo)

;; Stop RTC
(state/<invoke-db-worker :thread-api/rtc-stop)

;; Toggle auto-push
(state/<invoke-db-worker :thread-api/rtc-toggle-auto-push)
```

#### Browser Console Commands

> **Removed:** `window.logseq.api.rtc_debug_state()`, `window.logseq.api.rtc_force_sync()`, and `window.logseq.api.rtc_get_pending_ops()` are not present in the codebase. Use the built-in RTC Debug UI (accessible from within the app) or call `:thread-api/rtc-get-debug-state` via the worker state API instead.

### Logging Best Practices

```clojure
;; Use structured logging
(log/info :rtc-operation-applied
  {:operation-type (first op)
   :block-uuid (:block-uuid (last op))
   :timestamp (get-current-t)})

;; Log performance metrics
(worker-util/profile :apply-remote-ops
  (apply-remote-ops ...))

;; Error logging with context
(log/error :rtc-operation-failed
  {:error (ex-message e)
   :operation op
   :context {...}})
```

## Contributing Guidelines

### Code Style

1. **Follow Clojure conventions**:
   - Use kebab-case for names
   - Prefer pure functions
   - Use destructuring appropriately

2. **RTC-specific conventions**:
   - Prefix RTC functions with namespace
   - Use consistent operation naming
   - Add comprehensive docstrings

3. **Error handling**:
   - Use meaningful error messages
   - Include context in exceptions
   - Handle edge cases gracefully

### Pull Request Process

1. **Create feature branch** from `master`
2. **Write comprehensive tests** for new functionality
3. **Update documentation** if needed
4. **Test with multiple clients** for RTC features
5. **Submit PR** with detailed description

### Code Review Checklist

- [ ] Does it handle all edge cases?
- [ ] Are there sufficient tests?
- [ ] Is the operation format correct?
- [ ] Does it maintain backward compatibility?
- [ ] Is error handling comprehensive?
- [ ] Are performance implications considered?

## Findings: New Capabilities

The following capabilities exist in the current codebase but were not covered in earlier versions of this document:

### Extended Client Sync Directory

`src/main/frontend/worker/sync/` now contains 16 files (was 5). Notable additions:

- **`apply_txs.cljs`** — The core logic for applying remote transactions to the local DataScript database.
- **`handle_message.cljs`** — Routes incoming WebSocket messages to the appropriate handler.
- **`transport.cljs`** — Coerces/validates WebSocket messages using the Malli schema (`coerce-ws-client-message`, `coerce-ws-server-message`).
- **`download.cljs` / `upload.cljs`** — Handles full graph download from and upload to object storage (S3-compatible).
- **`presence.cljs`** — Client-side user-presence tracking (mirrors the server-side presence in `deps/db-sync`).
- **`auth.cljs`** — Client-side authentication handling.
- **`temp_sqlite.cljs`** — Manages temporary SQLite operations during sync.
- **`large_title.cljs`** — Handles blocks with large titles during sync.

### Thread-API RTС Operations

Available via `(state/<invoke-db-worker :thread-api/rtc-*)`:

- `:thread-api/rtc-start` / `:thread-api/rtc-stop` — Start and stop RTC.
- `:thread-api/rtc-toggle-auto-push` — Toggle automatic operation push.
- `:thread-api/rtc-toggle-remote-profile` — Toggle remote profiling.
- `:thread-api/rtc-grant-graph-access` — Grant collaborator access to a graph.
- `:thread-api/rtc-get-graphs` — List remote graphs.
- `:thread-api/rtc-delete-graph` — Delete a remote graph.
- `:thread-api/rtc-get-users-info` — Get user info for collaborators.
- `:thread-api/rtc-get-block-content-versions` — Fetch version history of a block.
- `:thread-api/rtc-get-debug-state` — Get the full RTC debug state map.
- `:thread-api/rtc-request-download-graph` / `:thread-api/rtc-download-graph-from-s3` — Trigger graph download.

### Transaction Integrity Checksums

`deps/db-sync/src/logseq/db_sync/checksum.cljs` provides checksum verification. `pull/ok` and `tx/batch/ok` server messages include an optional `:checksum` field for integrity verification.

### Graph End-to-End Encryption (E2EE)

The `graph-info-schema` includes `[:graph-e2ee? {:optional true} :boolean]`. Per-graph E2EE is supported; see `crypt.cljs`.

### Graph Schema Versioning

`graph-info-schema` includes `[:schema-version {:optional true} [:maybe :string]]`. The RTC debug state also tracks `:local-graph-schema-version` and `:remote-graph-schema-version` separately, enabling detection of version mismatches.

### Transaction Sanitization

`deps/db-sync/src/logseq/db_sync/tx_sanitize.cljs` strips deleted/migrated datoms from transactions before they are applied. When adding new DB migrations that delete properties, add those property identifiers to `logseq.db-sync.tx-sanitize/migration-deleted-attrs`.

## Resources

### Documentation

- [Logseq Architecture Overview](../ARCHITECTURE.md)
- [Database Schema Documentation](../DATABASE_SCHEMA.md)
- [WebSocket Protocol Specification](../WEBSOCKET_PROTOCOL.md)
- [Fractional Indexing Library](https://github.com/logseq/clj-fractional-indexing)

### External Resources

- [ClojureScript Guide](https://clojurescript.org/guides)
- [DataScript Documentation](https://github.com/tonsky/datascript)
- [Missionary (Async Library)](https://github.com/leonoel/missionary)
- [Real-time Collaboration Patterns](https://martinfowler.com/articles/patterns-of-real-time-web-architecture.html)

### Key Files to Understand

1. **Start here**: `deps/db-sync/src/logseq/db_sync/worker.cljs` - Main sync worker orchestration
2. **Operation handling**: `src/main/frontend/worker/sync/client_op.cljs` - Client-side operation management
3. **Sync handler**: `deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs` - Sync handling
4. **WebSocket layer**: `deps/db-sync/src/logseq/db_sync/worker/ws.cljs` - Communication protocol
5. **RTC flows**: `src/main/frontend/handler/db_based/rtc_flows.cljs` - Application-level RTC flows

### Development Tips

1. **Start small**: Begin with simple operations like block content updates
2. **Use logging liberally**: RTC is complex, good logging is essential
3. **Test with multiple browsers**: Open multiple tabs to simulate collaboration
4. **Understand the data flow**: Operations → Queue → WebSocket → Server → Broadcast → Apply
5. **Read existing tests**: They show expected behavior and edge cases

### Getting Help

- **Code questions**: Check existing issues or ask in development channels
- **Architecture questions**: Refer to this document and existing documentation
- **Bugs**: Create detailed reproduction steps with debug logs
- **Feature requests**: Discuss design first before implementing

---

Welcome to RTC development! This system is complex but powerful. Take time to understand the fundamentals before diving into implementation. Don't hesitate to ask questions and contribute to improving this documentation as you learn.

Good luck, and happy coding! 🚀
