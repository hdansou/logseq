# Logseq Real-Time Collaboration (RTC) Developer Onboarding Guide

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

```
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

### Main RTC Directory: `src/main/frontend/worker/rtc/`

```
rtc/
├── core.cljs                    # Main RTC orchestration and API
├── client.cljs                  # Client-side operation handling
├── client_op.cljs              # Operation storage and management
├── remote_update.cljs          # Applying remote changes
├── ws.cljs                     # WebSocket wrapper
├── ws_util.cljs                # WebSocket utilities
├── malli_schema.cljs           # Data validation schemas
├── db.cljs                     # RTC database operations
├── asset.cljs                  # Asset synchronization
├── branch_graph.cljs           # Graph branching/forking
├── full_upload_download_graph.cljs  # Graph upload/download
├── migrate.cljs                # Schema migrations
├── log_and_state.cljs          # Logging and state management
├── const.cljs                  # Constants and configuration
├── exception.cljs              # Error handling
├── gen_client_op.cljs          # Operation generation
├── hash.cljs                   # Content hashing
├── db_listener.cljs            # Database change listeners
├── asset_db_listener.cljs      # Asset change listeners
└── skeleton.cljs               # Placeholder/template code
```

### Related Files

- `src/main/frontend/fs/sync.cljs` - File synchronization (different from RTC)
- `deps/db/src/logseq/db/common/order.cljs` - Fractional indexing utilities
- `src/main/frontend/worker/rtc/` - All RTC worker code

## Key Concepts

### 1. Operations

Operations represent atomic changes to the graph:

```clojure
;; Operation types
[:update t {:block-uuid uuid :av-coll [...]}]    ; Update block content/properties
[:move t {:block-uuid uuid}]                     ; Move block to new position
[:remove t {:block-uuid uuid}]                   ; Delete block
[:update-page t {:block-uuid uuid}]              ; Update page properties
[:remove-page t {:block-uuid uuid}]              ; Delete page
[:update-asset t {:block-uuid uuid}]             ; Update asset
[:remove-asset t {:block-uuid uuid}]             ; Remove asset
[:update-kv-value t {:db-ident ident :value v}]  ; Update key-value data
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

Blocks use fractional indexes for ordering:

```clojure
;; Generate position between two blocks
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

```clojure
;; Outgoing messages
{:action "apply-ops" :ops [...] :graph-uuid uuid}
{:action "pull-remote-data" :graph-uuid uuid}
{:action "list-graphs"}

;; Incoming messages
{:req-id "push-updates" :value {...}}          ; Remote operations
{:req-id "online-users-updated" :value [...]}  ; User presence
{:req-id "push-asset-block-updates" :value {...}}  ; Asset updates
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
   npm install
   yarn install
   ```

3. **Start development server**:

   ```bash
   yarn watch  # or npm run watch
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

1. **Define the operation schema** in `malli_schema.cljs`:

   ```clojure
   [:my-new-op
    [:catn
     [:op :keyword]
     [:t :int]
     [:value [:map [:block-uuid :uuid] [:my-data :string]]]]]
   ```

2. **Add operation generation** in `gen_client_op.cljs`:

   ```clojure
   (defn generate-my-new-op [block-uuid my-data]
     [:my-new-op (get-current-t) {:block-uuid block-uuid :my-data my-data}])
   ```

3. **Handle operation in remote updates** in `remote_update.cljs`:

   ```clojure
   (defn apply-my-new-op [repo conn op-value]
     ;; Implementation here
     )
   ```

4. **Add to operation partitioning** in `client_op.cljs`

### 2. Debugging RTC Issues

1. **Enable debug logging**:

   ```clojure
   (js/console.log "RTC Debug:" data)
   ```

2. **Inspect operation queues**:

   ```clojure
   (client-op/get-all-block-ops repo)
   ```

3. **Check WebSocket state**:

   ```clojure
   ;; In browser console
   window.logseq.rtc.debug_state()
   ```

4. **Monitor transaction flow**:
   ```clojure
   ;; Add logging in core.cljs
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

```clojure
(let [op [:update (get-current-t) {:block-uuid block-uuid :av-coll av-coll}]]
  (client-op/add-ops! repo [op]))
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

```
test/
├── frontend/
│   └── worker/
│       └── rtc/
│           ├── core_test.cljs
│           ├── client_op_test.cljs
│           ├── remote_update_test.cljs
│           └── integration_test.cljs
```

### Running Tests

```bash
# Unit tests
yarn test:rtc

# Integration tests
yarn test:rtc-integration

# E2E tests
yarn test:rtc-e2e
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
;; Get current RTC state
(rtc-core/new-task--get-debug-state)

;; Check pending operations
(client-op/get-unpushed-block-ops-count repo)

;; Monitor WebSocket
(ws/create-mws-state-flow mws)
```

#### Browser Console Commands

```javascript
// Get RTC debug state
window.logseq.api.rtc_debug_state()

// Force sync
window.logseq.api.rtc_force_sync()

// Get operation queue
window.logseq.api.rtc_get_pending_ops()
```

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

1. **Start here**: `core.cljs` - Main orchestration
2. **Operation handling**: `client_op.cljs` - Local operation management
3. **Remote updates**: `remote_update.cljs` - Applying remote changes
4. **WebSocket layer**: `ws.cljs` - Communication protocol
5. **Conflict resolution**: Look at the `update-remote-data-by-local-unpushed-ops` function

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
